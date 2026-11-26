package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
)

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
			records, err := mergeFillsToRecords(leaderHistoryFills(ctx, tx, target))
			if err != nil {
				return nil, err
			}
			for _, body := range records {
				stamp, e := strconv.ParseInt(fmt.Sprint(body["orderTime"]), 10, 64)
				if e != nil {
					return nil, e
				}
				rows = append(rows, map[string]any{"target": target, "name": name, "kind": "order", "body": body, "stamp": stamp, "historical": 0})
			}
		}
		if kind == "leader" || kind == "transfer" {
			transfers, err := dashboardRows(ctx, tx, "SELECT target,name,kind,body,stamp,historical FROM events WHERE target=? AND kind='transfer'", target)
			if err != nil {
				return nil, err
			}
			for _, row := range transfers {
				body, err := DecodeRecord(row["body"])
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
			body, err := DecodeRecord(row["body"])
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
		if err = AttachDiscovery(ctx, tx, row, body); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return map[string]any{"list": rows, "total": total, "page": page}, nil
}
