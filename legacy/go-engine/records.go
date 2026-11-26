package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func decodeRecord(raw any) (map[string]any, error) {
	text, ok := raw.(string)
	if !ok {
		return nil, errors.New("invalid stored JSON")
	}
	var data map[string]any
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	err := decoder.Decode(&data)
	return data, err
}

func attachDiscovery(ctx context.Context, tx *sql.Tx, row map[string]any, body map[string]any) error {
	row["discovered_at_ms"], row["discovery_delay_ms"] = nil, nil
	if kind, ok := row["kind"]; ok && kind != "order" {
		return nil
	}
	target := row["target"]
	if target == nil {
		target = row["target_id"]
	}
	key, err := recordIdentity(body)
	if err != nil {
		return err
	}
	var discovered, delay int64
	err = tx.QueryRowContext(ctx, "SELECT discovered_at_ms,discovery_delay_ms FROM order_discovery WHERE target=? AND key=?", target, key).Scan(&discovered, &delay)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	row["discovered_at_ms"], row["discovery_delay_ms"] = discovered, delay
	return nil
}

func (s *SQLiteStore) CopyRecords(ctx context.Context, account int64, page, limit int) (map[string]any, error) {
	page = max(1, min(100000, page))
	limit = max(1, min(50, limit))
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var exists bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM futures_accounts WHERE id=?)", account).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, InputError("账号不存在")
	}
	var total int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM copy_tasks WHERE account_id=?", account).Scan(&total); err != nil {
		return nil, err
	}
	rows, err := dashboardRows(ctx, tx, `SELECT c.id,c.target_id,c.action,c.state,c.error,c.created,c.result,c.body,a.asset,a.wallet_balance,a.available_balance,a.lead_balance,a.planned_quantity,t.name AS target_name FROM copy_tasks c LEFT JOIN copy_task_audit a ON a.task_id=c.id LEFT JOIN targets t ON t.id=c.target_id WHERE c.account_id=? ORDER BY c.id DESC LIMIT ? OFFSET ?`, account, limit, (page-1)*limit)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		body, err := decodeRecord(row["body"])
		if err != nil {
			return nil, err
		}
		row["body"] = body
		if row["result"] != nil && row["result"] != "" {
			result, err := decodeRecord(row["result"])
			if err != nil {
				return nil, err
			}
			if result["orderId"] != nil {
				result["orderId"] = fmt.Sprint(result["orderId"])
			}
			row["result"] = result
		} else {
			row["result"] = nil
		}
		if err = attachDiscovery(ctx, tx, row, body); err != nil {
			return nil, err
		}
	}
	return map[string]any{"list": rows, "page": page, "total": total}, tx.Commit()
}

func recordIdentity(body map[string]any) (string, error) {
	stamp, err := strconv.ParseInt(fmt.Sprint(body["orderUpdateTime"]), 10, 64)
	created, e := strconv.ParseInt(fmt.Sprint(body["orderTime"]), 10, 64)
	var identityTime any = stamp
	if err != nil || e != nil || stamp <= 0 || stamp < created {
		identityTime = []any{"invalid", body["orderTime"]}
	}
	parts := make([]any, 0, 4)
	for _, field := range []string{"symbol", "side", "positionSide"} {
		value, _ := body[field].(string)
		parts = append(parts, strings.ToUpper(strings.TrimSpace(value)))
	}
	encoded, err := json.Marshal(append(parts, identityTime))
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(encoded)

	return hex.EncodeToString(hash[:]), nil
}
