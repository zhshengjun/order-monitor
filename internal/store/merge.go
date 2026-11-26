package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/big"
	"strings"

	"order-monitor/internal/domain"
)

// mergePersistedFills applies the Python merge rule to stored rows: rows of the
// same order (symbol, direction and orderTime) are unique executions, and rows
// that already carry notional/totalPnl are one cumulative fill.
func mergePersistedFills(rows []domain.LeaderFill) ([]domain.LeaderFill, error) {
	// Merge by symbol, direction and order creation time, matching Python's
	// merge_order_fills: every update of the same order (same orderTime) is one
	// cumulative fill, and distinct orders keep their own update-time identity.
	groups := map[[4]string][]domain.LeaderFill{}
	order := make([][4]string, 0, len(rows))
	for _, f := range rows {
		key := [4]string{strings.ToUpper(f.Symbol), strings.ToUpper(f.Side), strings.ToUpper(f.PositionSide), "c:" + strings.TrimSpace(f.Created.String())}
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], f)
	}
	merged := make([]domain.LeaderFill, 0, len(groups))
	for _, key := range order {
		entries := groups[key]
		if len(entries) == 1 {
			merged = append(merged, entries[0])
			continue
		}
		representative := entries[0]
		totalQty, totalCost, totalPnl := new(big.Rat), new(big.Rat), new(big.Rat)
		for _, e := range entries {
			qty, err := decimal(e.Quantity.String())
			if err != nil || qty.Sign() <= 0 {
				return nil, errors.New("合并成交数量无效")
			}
			cost, err := e.CumulativeNotional()
			if err != nil || cost.Sign() < 0 {
				return nil, errors.New("合并成交金额无效")
			}
			totalQty.Add(totalQty, qty)
			totalCost.Add(totalCost, cost)
			if e.Pnl != "" {
				if p, err := signedDecimal(e.Pnl.String()); err == nil {
					totalPnl.Add(totalPnl, p)
				}
			}
			if e.Updated != "" && e.Updated.String() > representative.Updated.String() {
				representative.Updated = e.Updated
			}
		}
		if totalQty.Sign() <= 0 {
			return nil, errors.New("合并成交数量无效")
		}
		average := decimalText(new(big.Rat).Quo(totalCost, totalQty))
		representative.Quantity = json.Number(decimalText(totalQty))
		representative.Price = json.Number(average)
		representative.Notional = json.Number(decimalText(totalCost))
		representative.Pnl = json.Number(decimalText(totalPnl))
		merged = append(merged, representative)
	}
	return merged, nil
}

// GroupLeaderEvents builds the leader trade list from stored rows, collapsing
// same-second fills into a single compounded record.
func leaderHistoryFills(ctx context.Context, tx *sql.Tx, target string) []domain.LeaderFill {
	rows, err := dashboardRows(ctx, tx, "SELECT body FROM leader_history WHERE target=? ORDER BY stamp,key", target)
	if err != nil {
		return nil
	}
	var fills []domain.LeaderFill
	for _, r := range rows {
		var f domain.LeaderFill
		if raw, ok := r["body"].(string); ok && json.Unmarshal([]byte(raw), &f) == nil {
			fills = append(fills, f)
		}
	}
	return fills
}

func (s *SQLiteStore) GroupLeaderEvents(ctx context.Context, target string) ([]map[string]any, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT body FROM leader_history WHERE target=? ORDER BY stamp,key", target)
	if err != nil {
		return nil, err
	}
	var fills []domain.LeaderFill
	for rows.Next() {
		var raw string
		var f domain.LeaderFill
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
	return mergeFillsToRecords(fills)
}

// MergeLeaderRecords collapses a slice of stored leader fills the same way the
// leader event list does, so notifications match what the page shows.
func MergeLeaderRecords(fills []domain.LeaderFill) ([]map[string]any, error) {
	return mergeFillsToRecords(fills)
}

func mergeFillsToRecords(fills []domain.LeaderFill) ([]map[string]any, error) {
	if len(fills) == 0 {
		return []map[string]any{}, nil
	}
	merged, err := mergePersistedFills(fills)
	if err != nil {
		return nil, err
	}
	records := make([]map[string]any, 0, len(merged))
	for _, f := range merged {
		raw, err := json.Marshal(f)
		if err != nil {
			return nil, err
		}
		record, err := DecodeRecord(string(raw))
		if err != nil {
			return nil, err
		}
		record["direction"] = direction(record)
		cost, err := f.CumulativeNotional()
		if err != nil {
			return nil, err
		}
		record["notional"] = decimalText(cost)
		records = append(records, record)
	}
	return records, nil
}
