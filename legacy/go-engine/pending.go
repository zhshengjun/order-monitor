package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type PendingCopy struct {
	ID, AccountID                                                            int64
	Target, OriginalKey, ClientID, Action, Before, LeaderBalance, Multiplier string
	APIKey, APISecret                                                        string
	Retries                                                                  int
	Body                                                                     struct {
		LeaderFill
		Incremental bool `json:"incremental"`
		Standalone  bool `json:"incremental_standalone"`
	}
}

func (s *SQLiteStore) NextPending(ctx context.Context, now time.Time) (PendingCopy, bool, error) {
	var task PendingCopy
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT c.id,c.account_id,c.target_id,c.order_key,c.client_id,c.action,c.before_qty,c.lead_balance,c.retry_count,c.body,a.api_key,a.api_secret,COALESCE(m.multiplier,'1')
	FROM copy_tasks c JOIN futures_accounts a ON a.id=c.account_id JOIN virtual_sessions v ON v.account_id=c.account_id AND v.target_id=c.target_id AND v.active=1
	JOIN targets t ON t.id=c.target_id AND t.enabled=1 LEFT JOIN copy_task_multiplier_v2 m ON m.task_id=c.id
	WHERE c.state='pending' AND c.next_check<=? AND NOT EXISTS(SELECT 1 FROM copy_tasks earlier WHERE earlier.account_id=c.account_id AND earlier.state='pending' AND earlier.id<c.id)
	ORDER BY c.id LIMIT 1`, now.Unix()).Scan(&task.ID, &task.AccountID, &task.Target, &task.OriginalKey, &task.ClientID, &task.Action, &task.Before, &task.LeaderBalance, &task.Retries, &raw, &task.APIKey, &task.APISecret, &task.Multiplier)
	if errors.Is(err, sql.ErrNoRows) {
		return task, false, nil
	}
	if err != nil {
		return task, false, err
	}
	if err = json.Unmarshal([]byte(raw), &task.Body); err != nil {
		return task, false, err
	}
	return task, true, nil
}

type CloseSnapshot struct {
	Tracked, Actual string
	Full            bool
}

type PreparedCopy struct {
	Order                         MarketOrder
	BalanceAt                     int64
	Wallet, Available             *string
	LeaderBalance, ReferencePrice string
	Close                         *CloseSnapshot
}

func (s *SQLiteStore) SubmitPending(ctx context.Context, task PendingCopy, plan PreparedCopy, client OrderExchange) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE copy_tasks SET state='submitted',quantity=? WHERE id=? AND state='pending'
	AND EXISTS(SELECT 1 FROM copy_live l JOIN copy_bindings b ON b.account_id=l.account_id AND b.target_id=l.target_id JOIN targets t ON t.id=l.target_id
	WHERE l.account_id=copy_tasks.account_id AND l.target_id=copy_tasks.target_id AND l.enabled=1 AND t.enabled=1)`, plan.Order.Quantity, task.ID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		res, err := tx.ExecContext(ctx, "UPDATE copy_tasks SET state='skipped',error='跟单关联或开关状态不一致，未提交订单' WHERE id=? AND state='pending'", task.ID)
		if err != nil {
			return err
		}
		if skipped, err := res.RowsAffected(); err != nil {
			return err
		} else if skipped == 1 {
			if err = copyNotice(ctx, tx, task.Target, map[string]any{"message": task.Body.Symbol + " 跟单跳过：跟单关联或开关状态不一致，未提交订单", "copy_task_id": task.ID}); err != nil {
				return err
			}
		}
		return tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO copy_task_audit VALUES(?,?,'USDT',?,?,?,?,?)", task.ID, plan.BalanceAt, plan.Wallet, plan.Available, plan.LeaderBalance, plan.ReferencePrice, plan.Order.Quantity); err != nil {
		return err
	}
	if plan.Close != nil {
		if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO copy_close_snapshots VALUES(?,?,?,?)", task.ID, plan.Close.Tracked, plan.Close.Actual, plan.Close.Full); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	result, err := client.Submit(ctx, plan.Order, task.ClientID)
	if err == nil {
		switch result.Status {
		case "FILLED", "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH":
			return s.SettleCopy(ctx, task.ID, result)
		default:
			_, err = s.db.ExecContext(ctx, "UPDATE copy_tasks SET next_check=strftime('%s','now')+15,error='订单结果待确认' WHERE id=? AND state='submitted'", task.ID)
			return err
		}
	}
	var exchangeErr ExchangeError
	state := "submitted"
	message := "下单结果未知，等待查询确认：" + err.Error()
	notice := "下单结果未知，正在查询订单状态：" + err.Error()
	if errors.As(err, &exchangeErr) && exchangeErr.DefinitelyRejected() {
		state = "skipped"
		message = err.Error()
		notice = task.Body.Symbol + " 跟单被交易所拒绝：" + err.Error()
	}
	recovery, dbErr := s.db.BeginTx(ctx, nil)
	if dbErr != nil {
		return errors.Join(err, dbErr)
	}
	defer recovery.Rollback()
	res, dbErr = recovery.ExecContext(ctx, "UPDATE copy_tasks SET state=?,next_check=strftime('%s','now')+15,error=? WHERE id=? AND state='submitted'", state, message, task.ID)
	if dbErr != nil {
		return errors.Join(err, dbErr)
	}
	n, dbErr = res.RowsAffected()
	if dbErr != nil {
		return errors.Join(err, dbErr)
	}
	if n > 0 {
		if dbErr = copyNotice(ctx, recovery, task.Target, map[string]any{"message": notice, "copy_task_id": task.ID}); dbErr != nil {
			return errors.Join(err, dbErr)
		}
	}
	return errors.Join(err, recovery.Commit())
}
