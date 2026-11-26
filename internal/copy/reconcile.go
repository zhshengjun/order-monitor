package copy

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type OrderLookup interface {
	Order(context.Context, string, string) (OrderResult, error)
}

// ReconcileNext uses the client ID already persisted by the original sender.
// Reserving the next check before the network request also bounds retries if
// the worker exits during the lookup.
func (s *Service) ReconcileNext(ctx context.Context, now time.Time, clientFor func(key, secret string) (OrderLookup, error)) (bool, error) {
	var id int64
	var attempts int
	var symbolBody, clientID, key, secret, target, previousError string
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	err = tx.QueryRowContext(ctx, `SELECT c.id,c.body,c.client_id,c.target_id,c.error,c.reconcile_attempts,a.api_key,a.api_secret FROM copy_tasks c JOIN futures_accounts a ON a.id=c.account_id WHERE c.state='submitted' AND c.next_check<=? ORDER BY c.id LIMIT 1`, now.Unix()).Scan(&id, &symbolBody, &clientID, &target, &previousError, &attempts, &key, &secret)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	delay := 15 * time.Second
	for i := 0; i < min(attempts, 4); i++ {
		delay *= 2
	}
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	if _, err = tx.ExecContext(ctx, "UPDATE copy_tasks SET next_check=?,reconcile_attempts=? WHERE id=?", now.Add(delay).Unix(), attempts+1, id); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	var body struct{ Symbol string }
	if err = json.Unmarshal([]byte(symbolBody), &body); err != nil {
		return true, err
	}
	client, err := clientFor(key, secret)
	if err != nil {
		return true, err
	}
	result, err := client.Order(ctx, body.Symbol, clientID)
	message := "订单结果待确认"
	nonTerminal := false
	if err != nil {
		message = "订单结果未知，等待查询确认：" + err.Error()
	} else {
		switch result.Status {
		case "FILLED", "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH":
			if err = s.SettleCopy(ctx, id, result); err == nil {
				return true, nil
			}
			message = "订单响应与提交信息不一致，等待人工核对"
		default:
			nonTerminal = true
		}
	}
	res, saveErr := s.DB().ExecContext(ctx, "UPDATE copy_tasks SET error=? WHERE id=? AND state='submitted'", message, id)
	if saveErr == nil {
		if n, _ := res.RowsAffected(); n == 1 {
			if previousError == "" && nonTerminal {
				saveErr = s.CopyNotice(ctx, target, map[string]any{"message": body.Symbol + " 订单结果待确认，系统只查询原订单，其他跟单继续执行", "copy_task_id": id})
			} else if attempts+1 == 8 {
				saveErr = s.CopyNotice(ctx, target, map[string]any{"message": body.Symbol + " 订单结果长时间无法确认，请人工核对", "copy_task_id": id})
			}
		}
	}
	return true, errors.Join(err, saveErr)

}
