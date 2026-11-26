package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
)

func (h *HistoryWorker) Delete(ctx context.Context, isTarget bool, item any, client *http.Client) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if isTarget {
		value, ok := item.(string)
		if !ok || !portfolioID.MatchString(value) {
			return InputError("带单员参数错误")
		}
	} else {
		value, ok := item.(int64)
		if !ok || value <= 0 {
			return InputError("账号参数错误")
		}
	}
	column, table, kind := "account_id", "futures_accounts", "accounts"
	if isTarget {
		column, table, kind = "target_id", "targets", "targets"
	}
	tx, err := h.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var exists, manual, pending bool
	err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM "+table+" WHERE id=?),EXISTS(SELECT 1 FROM manual_closes WHERE "+column+"=? AND state IN ('submitted','needs_review')),EXISTS(SELECT 1 FROM copy_tasks WHERE "+column+"=? AND state IN ('pending','submitted'))", item, item, item).Scan(&exists, &manual, &pending)
	if err != nil {
		return err
	}
	if !exists {
		return InputError("对象不存在")
	}
	if manual {
		return InputError("存在待确认的手动平仓，无法删除")
	}
	if pending {
		return InputError("存在待执行或待确认订单，无法删除")
	}
	positions, err := dashboardRows(ctx, tx, "SELECT account_id,symbol,position_side,quantity FROM copied_positions WHERE "+column+"=?", item)
	if err != nil {
		return err
	}
	snapshots := map[int64][]FuturesPosition{}
	for _, position := range positions {
		qty, err := decimal(position["quantity"].(string))
		if err != nil {
			return err
		}
		if qty.Sign() == 0 {
			continue
		}
		if !isTarget {
			return InputError("存在跟单持仓，请先核对并处理后再删除")
		}
		account := position["account_id"].(int64)
		snapshot, ok := snapshots[account]
		if !ok {
			var key, secret string
			err = tx.QueryRowContext(ctx, "SELECT api_key,api_secret FROM futures_accounts WHERE id=?", account).Scan(&key, &secret)
			if errors.Is(err, sql.ErrNoRows) {
				return InputError("跟单持仓关联账户不存在，无法核对，请先处理")
			}
			if err != nil {
				return err
			}
			c, err := NewFuturesClient(client, "https://fapi.binance.com", key, secret)
			if err != nil {
				return err
			}
			snapshot, err = c.Positions(ctx)
			if err != nil {
				return InputError("无法核对交易所实际持仓，请检查账户连接后重试")
			}
			for _, p := range snapshot {
				if p.Symbol == "" || (p.Side != "BOTH" && p.Side != "LONG" && p.Side != "SHORT") {
					return InputError("无法核对交易所实际持仓，请检查账户连接后重试")
				}
				if _, err := signedDecimal(p.Amount); err != nil {
					return InputError("无法核对交易所实际持仓，请检查账户连接后重试")
				}
			}
			snapshots[account] = snapshot
		}
		symbol, side := position["symbol"].(string), position["position_side"].(string)
		actual, err := PositionAmount(snapshot, symbol, side)
		if err != nil {
			return InputError("无法核对交易所实际持仓，请检查账户连接后重试")
		}
		value, err := decimal(actual)
		if err != nil {
			return err
		}
		if value.Sign() > 0 {
			return InputError(fmt.Sprintf("账户存在 %s %s 实际持仓，请先核对并处理后再删除", symbol, side))
		}
	}
	for _, query := range []string{"DELETE FROM copy_task_audit WHERE task_id IN (SELECT id FROM copy_tasks WHERE " + column + "=?)", "DELETE FROM copy_task_multiplier_v2 WHERE task_id IN (SELECT id FROM copy_tasks WHERE " + column + "=?)", "DELETE FROM copy_multiplier_settings_v2 WHERE " + column + "=?", "DELETE FROM manual_closes WHERE " + column + "=?"} {
		if _, err = tx.ExecContext(ctx, query, item); err != nil {
			return err
		}
	}
	for _, name := range []string{"close_watch", "copy_tasks", "copied_positions", "copy_multipliers", "copy_bindings", "copy_live", "virtual_sessions", "virtual_positions", "virtual_events"} {
		if _, err = tx.ExecContext(ctx, "DELETE FROM "+name+" WHERE "+column+"=?", item); err != nil {
			return err
		}
	}
	if isTarget {
		if _, err = tx.ExecContext(ctx, "DELETE FROM outbox WHERE event IN (SELECT id FROM events WHERE target=?)", item); err != nil {
			return err
		}
		for _, name := range []string{"records", "events", "order_discovery", "leader_history", "leader_history_stage", "leader_history_sync", "baseline_watermarks", "monitor_start"} {
			if _, err = tx.ExecContext(ctx, "DELETE FROM "+name+" WHERE target=?", item); err != nil {
				return err
			}
		}
		// Remove persisted Go baselines so a subsequently re-added leader starts fresh.
		for _, table := range []string{"go_fill_baselines", "go_legacy_order_keys"} {
			if _, err = tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE json_extract(order_key,'$.Portfolio')=?", item); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM go_fill_work WHERE json_extract(body,'$.Key.Portfolio')=?", item); err != nil {
			return err
		}
	} else {
		if _, err = tx.ExecContext(ctx, "DELETE FROM margin_alert_state WHERE account_id=?", item); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM display_order WHERE kind=? AND item_id=?", kind, fmt.Sprint(item)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE id=?", item); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if isTarget {
		delete(h.active, item.(string))
	}
	return nil
}
