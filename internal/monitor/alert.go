package monitor

import (
	"context"
	"fmt"
	"order-monitor/internal/store"
)

// alertInvalidFill mirrors Python's stale branch for fills whose order update
// time is missing or invalid: never trade, but tell the operator and watch for
// a possible full close.
func (s *Service) alertInvalidFill(ctx context.Context, target string, fill LeaderFill) error {
	var live bool
	if err := s.DB().QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM copy_live WHERE target_id=? AND enabled=1)", target).Scan(&live); err != nil {
		return err
	}
	if !live {
		return nil
	}
	closed := (fill.Side == "SELL" && fill.PositionSide == "LONG") || (fill.Side == "BUY" && fill.PositionSide == "SHORT")
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT account_id FROM copy_live WHERE target_id=? AND enabled=1", target)
	if err != nil {
		return err
	}
	var accounts []int64
	for rows.Next() {
		var account int64
		if err = rows.Scan(&account); err != nil {
			rows.Close()
			return err
		}
		accounts = append(accounts, account)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, account := range accounts {
		if err = store.SetVirtualTrust(ctx, tx, account, target, false, "订单更新时间无效，成交未跟单"); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err := s.CopyNotice(ctx, target, map[string]any{"message": fmt.Sprintf("%s %s 跟单跳过：订单更新时间缺失、无效或超过三分钟，请核对仓位", fill.Symbol, Direction(map[string]any{"side": fill.Side, "positionSide": fill.PositionSide}))}); err != nil {
		return err
	}
	if !closed {
		return nil
	}
	executed, err := positiveDecimal(fill.Quantity.String(), "leader execution quantity")
	if err != nil {
		return err
	}
	record := map[string]any{"symbol": fill.Symbol, "side": fill.Side, "positionSide": fill.PositionSide, "orderTime": fill.Created, "orderUpdateTime": fill.Updated}
	key, err := RecordIdentity(record)
	if err != nil {
		return err
	}
	rows, err = s.DB().QueryContext(ctx, `SELECT v.account_id,v.quantity FROM virtual_positions v
	JOIN copy_live l ON l.account_id=v.account_id AND l.target_id=v.target_id AND l.enabled=1
	WHERE v.target_id=? AND v.symbol=? AND v.position_side=?`, target, fill.Symbol, fill.PositionSide)
	if err != nil {
		return err
	}
	type position struct {
		account  int64
		quantity string
	}
	var positions []position
	for rows.Next() {
		var p position
		if err = rows.Scan(&p.account, &p.quantity); err != nil {
			rows.Close()
			return err
		}
		positions = append(positions, p)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, p := range positions {
		qty, err := decimal(p.quantity)
		if err != nil {
			return err
		}
		if qty.Sign() > 0 && executed.Cmp(qty) >= 0 {
			if _, err = s.DB().ExecContext(ctx, "INSERT OR IGNORE INTO close_watch(account_id,target_id,symbol,position_side,order_key) VALUES(?,?,?,?,?)", p.account, target, fill.Symbol, fill.PositionSide, key); err != nil {
				return err
			}
		}
	}
	return nil
}
