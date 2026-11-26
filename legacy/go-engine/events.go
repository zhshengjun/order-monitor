package engine

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"sort"
)

func direction(body map[string]any) string {
	side, _ := body["side"].(string)
	position, _ := body["positionSide"].(string)
	result := map[[2]string]string{{"BUY", "LONG"}: "开多", {"SELL", "LONG"}: "平多", {"SELL", "SHORT"}: "开空", {"BUY", "SHORT"}: "平空"}[[2]string{side, position}]
	if result == "" {
		return side
	}
	return result
}
func (s *SQLiteStore) Events(ctx context.Context, target, kind string, page, limit int) (map[string]any, error) {
	page = max(1, min(page, 100000))
	limit = max(1, min(limit, 50))
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var rows []map[string]any
	total := 0
	var name string
	err = tx.QueryRowContext(ctx, "SELECT name FROM targets WHERE id=?", target).Scan(&name)
	historical := err == nil && target != "" && (kind == "leader" || kind == "order" || kind == "transfer")
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if historical {
		rows = []map[string]any{}
		if kind == "leader" || kind == "order" {
			raw, err := dashboardRows(ctx, tx, "SELECT body,stamp FROM leader_history WHERE target=?", target)
			if err != nil {
				return nil, err
			}
			for _, r := range raw {
				body, err := decodeRecord(r["body"])
				if err != nil {
					return nil, err
				}
				body["direction"] = direction(body)
				if _, ok := body["notional"]; !ok {
					q, e := decimal(fmt.Sprint(body["executedQty"]))
					if e != nil {
						return nil, e
					}
					p, e := decimal(fmt.Sprint(body["avgPrice"]))
					if e != nil {
						return nil, e
					}
					body["notional"] = decimalString(new(big.Rat).Mul(q, p))
				}
				rows = append(rows, map[string]any{"target": target, "name": name, "kind": "order", "body": body, "stamp": r["stamp"], "historical": 0})
			}
		}
		if kind == "leader" || kind == "transfer" {
			transfers, err := dashboardRows(ctx, tx, "SELECT target,name,kind,body,stamp,historical FROM events WHERE target=? AND kind='transfer'", target)
			if err != nil {
				return nil, err
			}
			for _, row := range transfers {
				body, err := decodeRecord(row["body"])
				if err != nil {
					return nil, err
				}
				row["body"] = body
				rows = append(rows, row)
			}
		}
		sort.SliceStable(rows, func(i, j int) bool {
			a, b := rows[i], rows[j]
			if a["stamp"].(int64) != b["stamp"].(int64) {
				return a["stamp"].(int64) > b["stamp"].(int64)
			}
			if a["kind"] != b["kind"] {
				return a["kind"].(string) > b["kind"].(string)
			}
			return fmt.Sprint(a["body"].(map[string]any)["symbol"]) > fmt.Sprint(b["body"].(map[string]any)["symbol"])
		})
		total = len(rows)
		start := min((page-1)*limit, total)
		end := min(start+limit, total)
		rows = rows[start:end]
	} else {
		where := "kind IN ('order','transfer','copy')"
		args := []any{}
		if kind == "leader" {
			where = "kind IN ('order','transfer')"
			kind = ""
		}
		if target != "" {
			where += " AND target=?"
			args = append(args, target)
		}
		if kind != "" {
			where += " AND kind=?"
			args = append(args, kind)
		}
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE "+where, args...).Scan(&total); err != nil {
			return nil, err
		}
		args = append(args, limit, (page-1)*limit)
		rows, err = dashboardRows(ctx, tx, "SELECT * FROM events WHERE "+where+" ORDER BY stamp DESC,id DESC LIMIT ? OFFSET ?", args...)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			body, err := decodeRecord(row["body"])
			if err != nil {
				return nil, err
			}
			if row["kind"] == "order" {
				body["direction"] = direction(body)
			}
			row["body"] = body
		}
	}
	for _, row := range rows {
		body := row["body"].(map[string]any)
		if err = attachDiscovery(ctx, tx, row, body); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return map[string]any{"list": rows, "total": total, "page": page}, nil
}
