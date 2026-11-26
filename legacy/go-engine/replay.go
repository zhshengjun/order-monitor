package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

type EstimatedPosition struct {
	Symbol    string  `json:"symbol"`
	Side      string  `json:"side"`
	Quantity  *string `json:"quantity"`
	Average   *string `json:"avg_price"`
	Uncertain bool    `json:"uncertain"`
}

type replayLedger struct {
	quantity, cost        big.Rat
	stamp                 int64
	open, seen, uncertain bool
}

func replayPositions(fills []LeaderFill) ([]EstimatedPosition, error) {
	type stamped struct {
		fill  LeaderFill
		stamp int64
	}
	ordered := make([]stamped, len(fills))
	for i, f := range fills {
		stamp, err := strconv.ParseInt(f.Created.String(), 10, 64)
		if err != nil {
			return nil, err
		}
		ordered[i] = stamped{f, stamp}
	}
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].stamp < ordered[j].stamp })
	positions := map[[2]string]*replayLedger{}
	for _, entry := range ordered {
		f := entry.fill
		if (f.Side != "BUY" && f.Side != "SELL") || (f.PositionSide != "LONG" && f.PositionSide != "SHORT") {
			return nil, InputError("交易方向无效，无法重放持仓")
		}
		quantity, err := decimal(f.Quantity.String())
		if err != nil {
			return nil, err
		}
		if quantity.Sign() == 0 {
			continue
		}
		price, err := positiveDecimal(f.Price.String(), "成交价格")
		if err != nil {
			return nil, err
		}
		key := [2]string{f.Symbol, f.PositionSide}
		p := positions[key]
		if p == nil {
			p = &replayLedger{}
			positions[key] = p
		}
		opening := (f.Side == "BUY") == (f.PositionSide == "LONG")
		simultaneous := p.seen && p.stamp == entry.stamp && p.open != opening
		p.seen, p.open, p.stamp = true, opening, entry.stamp
		if simultaneous {
			p.uncertain = true
		}
		switch {
		case opening:
			p.quantity.Add(&p.quantity, quantity)
			p.cost.Add(&p.cost, new(big.Rat).Mul(quantity, price))
		case quantity.Cmp(&p.quantity) > 0:
			p.quantity.SetInt64(0)
			p.cost.SetInt64(0)
			p.uncertain = simultaneous
		default:
			remainder := new(big.Rat).Sub(&p.quantity, quantity)
			p.cost.Mul(&p.cost, new(big.Rat).Quo(remainder, &p.quantity))
			p.quantity.Set(remainder)
			if remainder.Sign() == 0 && !simultaneous {
				p.uncertain = false
			}
		}
	}
	result := []EstimatedPosition{}
	for key, p := range positions {
		if p.quantity.Sign() == 0 && !p.uncertain {
			continue
		}
		row := EstimatedPosition{Symbol: key[0], Side: key[1], Uncertain: p.uncertain}
		if !p.uncertain {
			q := decimalString(&p.quantity)
			row.Quantity = &q
			price := new(big.Rat).Quo(&p.cost, &p.quantity)
			value := decimalString(price)
			if _, exact := price.FloatPrec(); !exact {
				value = strings.TrimRight(strings.TrimRight(price.FloatString(40), "0"), ".")
			}
			row.Average = &value
		}
		result = append(result, row)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Symbol == result[j].Symbol {
			return result[i].Side < result[j].Side
		}
		return result[i].Symbol < result[j].Symbol
	})
	return result, nil
}

func (s *SQLiteStore) LeaderPositions(ctx context.Context, target string, historyActive bool) (map[string]any, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	monitor, err := dashboardRows(ctx, tx, "SELECT enabled,error,last_check FROM targets WHERE id=?", target)
	if err != nil {
		return nil, err
	}
	if len(monitor) == 0 {
		return nil, InputError("带单员不存在")
	}
	rows, err := tx.QueryContext(ctx, "SELECT body FROM leader_history WHERE target=? ORDER BY stamp,key", target)
	if err != nil {
		return nil, err
	}
	var fills []LeaderFill
	for rows.Next() {
		var raw string
		var f LeaderFill
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal([]byte(raw), &f)
		}
		if err != nil {
			rows.Close()
			return nil, err
		}
		fills = append(fills, f)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	syncRows, err := dashboardRows(ctx, tx, "SELECT page,complete,total,error,updated,next_check,failures FROM leader_history_sync WHERE target=?", target)
	if err != nil {
		return nil, err
	}
	var sync any
	if len(syncRows) > 0 {
		sync = syncRows[0]
	}
	var staged int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM leader_history_stage WHERE target=?", target).Scan(&staged); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	positions, err := replayPositions(fills)
	if err != nil {
		return nil, err
	}
	return map[string]any{"positions": positions, "count": len(fills), "staged": staged, "sync": sync, "monitor": monitor[0], "history_active": historyActive}, nil
}
