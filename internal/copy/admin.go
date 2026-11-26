package copy

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"order-monitor/internal/exchange"
)

// PositionAdjustment corrects the locally attributed copy position after an
// operator has reconciled a manual change on the exchange.
type PositionAdjustment struct {
	AccountID    int64  `json:"account_id"`
	TargetID     string `json:"target_id"`
	Symbol       string `json:"symbol"`
	PositionSide string `json:"position_side"`
	Quantity     string `json:"quantity"`
}

// ResolveSubmittedCopy only finishes a task when the exchange explicitly says
// the original client order does not exist. It never resends an order.
func (s *Service) ResolveSubmittedCopy(ctx context.Context, taskID int64, clientFor func(key, secret string) (OrderLookup, error)) (string, error) {
	var account int64
	var target, state, clientID, bodyRaw, key, secret string
	err := s.DB().QueryRowContext(ctx, `SELECT c.account_id,c.target_id,c.state,c.client_id,c.body,a.api_key,a.api_secret
	FROM copy_tasks c JOIN futures_accounts a ON a.id=c.account_id WHERE c.id=?`, taskID).Scan(&account, &target, &state, &clientID, &bodyRaw, &key, &secret)
	if errors.Is(err, sql.ErrNoRows) {
		return "", InputError("跟单记录不存在")
	}
	if err != nil {
		return "", err
	}
	if state != "submitted" {
		return "", InputError("只有结果待确认的订单可以核对")
	}
	var body struct {
		Symbol string `json:"symbol"`
	}
	if err = json.Unmarshal([]byte(bodyRaw), &body); err != nil || body.Symbol == "" {
		return "", InputError("跟单记录缺少交易对")
	}
	client, err := clientFor(key, secret)
	if err != nil {
		return "", err
	}
	result, err := client.Order(ctx, body.Symbol, clientID)
	if err == nil {
		switch result.Status {
		case "FILLED", "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH":
			if err = s.SettleCopy(ctx, taskID, result); err != nil {
				return "", err
			}
			return "settled", nil
		default:
			return "", InputError("交易所订单仍在进行中，不能结束")
		}
	}
	var rejection ExchangeError
	if !errors.As(err, &rejection) || rejection.Status != http.StatusBadRequest || (rejection.Code != -2013 && rejection.Code != -2011) {
		return "", fmt.Errorf("无法确认订单状态，请稍后重试：%w", err)
	}
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, "UPDATE copy_tasks SET state='skipped',error=? WHERE id=? AND state='submitted'", "人工核对：交易所无此订单，已结束任务", taskID)
	if err != nil {
		return "", err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return "", err
	}
	if n == 1 {
		if err = copyNotice(ctx, tx, target, map[string]any{"message": body.Symbol + " 人工核对：交易所无此订单，任务已结束", "copy_task_id": taskID}); err != nil {
			return "", err
		}
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	return "abandoned", nil
}

// AdjustCopyPosition corrects the locally recorded attribution for one
// account/leader/symbol/side pair. In-flight tasks are rejected to avoid a race.
func (s *Service) AdjustCopyPosition(ctx context.Context, in PositionAdjustment) error {
	symbol := strings.ToUpper(strings.TrimSpace(in.Symbol))
	side := strings.ToUpper(strings.TrimSpace(in.PositionSide))
	if in.AccountID <= 0 || in.TargetID == "" || symbol == "" || (side != "LONG" && side != "SHORT") {
		return InputError("账号、带单员、交易对或方向无效")
	}
	quantity, err := decimal(in.Quantity)
	if err != nil {
		return InputError("数量必须为非负数字")
	}
	var bound bool
	if err = s.DB().QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM copy_bindings WHERE account_id=? AND target_id=?)", in.AccountID, in.TargetID).Scan(&bound); err != nil {
		return err
	}
	if !bound {
		return InputError("请先关联带单员")
	}
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var inFlight bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM copy_tasks WHERE account_id=? AND target_id=? AND state IN ('pending','submitted'))", in.AccountID, in.TargetID).Scan(&inFlight); err != nil {
		return err
	}
	if inFlight {
		return InputError("该关联存在待执行或待确认订单，不能修正持仓")
	}
	var previous string
	err = tx.QueryRowContext(ctx, "SELECT quantity FROM copied_positions WHERE account_id=? AND target_id=? AND symbol=? AND position_side=?", in.AccountID, in.TargetID, symbol, side).Scan(&previous)
	if errors.Is(err, sql.ErrNoRows) {
		previous = "0"
	} else if err != nil {
		return err
	}
	next := decimalString(quantity)
	if _, err = tx.ExecContext(ctx, `INSERT INTO copied_positions(account_id,target_id,symbol,position_side,quantity) VALUES(?,?,?,?,?)
	ON CONFLICT(account_id,target_id,symbol,position_side) DO UPDATE SET quantity=excluded.quantity`, in.AccountID, in.TargetID, symbol, side, next); err != nil {
		return err
	}
	if err = copyNotice(ctx, tx, in.TargetID, map[string]any{"message": fmt.Sprintf("%s %s 人工修正跟单持仓：%s → %s", symbol, side, previous, next)}); err != nil {
		return err
	}
	return tx.Commit()
}

// ClientFactory builds an authenticated futures client for a saved account.
func ClientFactory(httpClient *http.Client) func(key, secret string) (OrderLookup, error) {
	return func(key, secret string) (OrderLookup, error) {
		return exchange.NewFuturesClient(httpClient, "https://fapi.binance.com", key, secret)
	}
}
