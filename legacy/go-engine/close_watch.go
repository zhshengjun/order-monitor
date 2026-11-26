package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"
)

func (s *SQLiteStore) checkCloseWatch(ctx context.Context, client *http.Client) (bool, error) {
	var id, account, created int64
	var target, symbol, side, name, key, secret string
	var taskID sql.NullInt64
	var state sql.NullString
	var notified bool
	err := s.db.QueryRowContext(ctx, `SELECT w.id,w.account_id,w.target_id,w.symbol,w.position_side,a.name,a.api_key,a.api_secret,w.task_id,c.state,COALESCE(c.created,0),w.error_notified FROM close_watch w JOIN futures_accounts a ON a.id=w.account_id LEFT JOIN copy_tasks c ON c.id=w.task_id WHERE w.state='pending' AND w.next_check<=? ORDER BY w.id LIMIT 1`, time.Now().Unix()).Scan(&id, &account, &target, &symbol, &side, &name, &key, &secret, &taskID, &state, &created, &notified)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if taskID.Valid && !state.Valid {
		return false, nil
	}
	if state.Valid && (state.String == "pending" || state.String == "submitted") && time.Now().UnixMilli()-created < 30000 {
		_, err = s.db.ExecContext(ctx, "UPDATE close_watch SET next_check=? WHERE id=? AND state='pending'", time.Now().Unix()+5, id)
		return true, err
	}
	c, err := NewFuturesClient(client, "https://fapi.binance.com", key, secret)
	var quantity string
	if err == nil {
		var positions []FuturesPosition
		positions, err = c.Positions(ctx)
		if err == nil {
			for _, p := range positions {
				if p.Symbol == "" || (p.Side != "LONG" && p.Side != "SHORT" && p.Side != "BOTH") {
					err = errors.New("持仓响应无效")
					break
				}
				if _, e := signedDecimal(p.Amount); e != nil {
					err = e
					break
				}
			}
			if err == nil {
				quantity, err = PositionAmount(positions, symbol, side)
			}
		}
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return false, e
	}
	defer tx.Rollback()
	if err != nil {
		res, e := tx.ExecContext(ctx, "UPDATE close_watch SET next_check=?,error_notified=1 WHERE id=? AND state='pending'", time.Now().Unix()+30, id)
		if e != nil {
			return false, e
		}
		n, e := res.RowsAffected()
		if e != nil {
			return false, e
		}
		if n > 0 && !notified {
			if e = copyNotice(ctx, tx, target, map[string]any{"message": fmt.Sprintf("%s %s 疑似已清仓，但无法查询账户 %s 的实际持仓，请核对", symbol, side, name)}); e != nil {
				return false, e
			}
		}
		return true, tx.Commit()
	}
	res, e := tx.ExecContext(ctx, "UPDATE close_watch SET state='checked' WHERE id=? AND state='pending'", id)
	if e != nil {
		return false, e
	}
	n, e := res.RowsAffected()
	if e != nil {
		return false, e
	}
	amount, e := decimal(quantity)
	if e != nil {
		return false, e
	}
	if n > 0 && amount.Sign() > 0 {
		suffix := ""
		if state.Valid && (state.String == "pending" || state.String == "submitted") {
			suffix = "，跟单任务仍待执行或确认"
		}
		msg := fmt.Sprintf("%s %s 带单员疑似已清仓；核对时账户 %s 仍有 %s 持仓%s，请检查（可能包含其他来源的仓位）", symbol, side, name, quantity, suffix)
		if e = copyNotice(ctx, tx, target, map[string]any{"message": msg}); e != nil {
			return false, e
		}
	}
	return true, tx.Commit()
}

func (s *SQLiteStore) RunCloseWatch(ctx context.Context, client *http.Client) {
	for ctx.Err() == nil {
		_, err := s.checkCloseWatch(ctx, client)
		if ctx.Err() != nil {
			return
		}
		if err != nil { /* retry the durable record on the next pass */
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
