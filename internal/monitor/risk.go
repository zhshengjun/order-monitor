package monitor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"sort"
	"strings"
	"time"

	"order-monitor/internal/exchange"
	"order-monitor/internal/store"
)

const riskInterval = 60 * time.Second
const healthInterval = 5 * time.Minute

// RunRiskMonitor performs periodic account health checks and position
// reconciliation. Over-attribution and invalid keys trip an account breaker.
func (s *Service) RunRiskMonitor(ctx context.Context, client *http.Client) {
	s.runHealth(ctx, client)
	s.runReconcile(ctx, client)
	risk := time.NewTicker(riskInterval)
	defer risk.Stop()
	health := time.NewTicker(healthInterval)
	defer health.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-risk.C:
			s.runReconcile(ctx, client)
		case <-health.C:
			s.runHealth(ctx, client)
		}
	}
}

func (s *Service) accounts(ctx context.Context) ([]struct {
	ID     int64
	Name   string
	Key    string
	Secret string
}, error) {
	rows, err := s.DB().QueryContext(ctx, "SELECT id,name,api_key,api_secret FROM futures_accounts ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var accounts []struct {
		ID     int64
		Name   string
		Key    string
		Secret string
	}
	for rows.Next() {
		var a struct {
			ID     int64
			Name   string
			Key    string
			Secret string
		}
		if err = rows.Scan(&a.ID, &a.Name, &a.Key, &a.Secret); err != nil {
			return nil, err
		}
		accounts = append(accounts, a)
	}
	return accounts, rows.Err()
}

func (s *Service) runHealth(ctx context.Context, client *http.Client) {
	accounts, err := s.accounts(ctx)
	if err != nil {
		log.Printf("账户健康检查读取账号失败：%v", err)
		return
	}
	for _, account := range accounts {
		status, checkErr := exchange.CheckAccount(ctx, client, account.Key, account.Secret)
		accountID := account.ID
		ok := checkErr == nil && status.CanTrade
		message := ""
		if checkErr != nil {
			message = checkErr.Error()
		} else if !status.CanTrade {
			message = "API Key 没有合约交易权限"
		}
		failures, saveErr := s.SaveAccountHealth(ctx, accountID, ok, status.CanTrade, status.CanWithdraw, message)
		if saveErr != nil {
			log.Printf("保存账户健康状态失败 account=%d: %v", accountID, saveErr)
			continue
		}
		if !ok {
			authFailure := false
			var exchangeErr exchange.ExchangeError
			if errors.As(checkErr, &exchangeErr) {
				authFailure = exchangeErr.Status == 401 || exchangeErr.Status == 403 || exchangeErr.Code == -2015 || exchangeErr.Code == -1002 || exchangeErr.Code == -1022
			}
			severity := store.SeverityWarning
			if authFailure || failures >= 3 {
				severity = store.SeverityCritical
			}
			s.raise(ctx, store.AlertSpec{Key: fmt.Sprintf("account-health:%d", accountID), Scope: fmt.Sprintf("account:%d", accountID), Severity: severity, Title: "API Key 异常", Message: fmt.Sprintf("%s API Key 异常（连续 %d 次）：%s", account.Name, failures, message)})
			if authFailure || failures >= 3 {
				s.trip(ctx, fmt.Sprintf("account:%d", accountID), "API Key 异常："+message)
			}
			continue
		}
		s.resolve(ctx, fmt.Sprintf("account-health:%d", accountID), "API Key 恢复正常")
	}
}

func (s *Service) raise(ctx context.Context, spec store.AlertSpec) {
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		log.Printf("告警写入失败 %s: %v", spec.Key, err)
		return
	}
	defer tx.Rollback()
	if _, err = s.RaiseAlert(ctx, tx, spec); err != nil {
		log.Printf("告警写入失败 %s: %v", spec.Key, err)
		return
	}
	if err = tx.Commit(); err != nil {
		log.Printf("告警提交失败 %s: %v", spec.Key, err)
	}
}

func (s *Service) resolve(ctx context.Context, key, detail string) {
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()
	if _, err = s.ResolveAlert(ctx, tx, key, detail); err != nil {
		return
	}
	_ = tx.Commit()
}

// trip pauses one account or one leader/target and records the reason.
func (s *Service) trip(ctx context.Context, scope, reason string) {
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()
	if err = s.tripTx(ctx, tx, scope, reason); err != nil {
		log.Printf("风控暂停失败 scope=%s: %v", scope, err)
		return
	}
	if err = tx.Commit(); err != nil {
		log.Printf("风控暂停提交失败 scope=%s: %v", scope, err)
	}
}

func (s *Service) tripTx(ctx context.Context, tx *sql.Tx, scope, reason string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO risk_breakers(scope,reason,active,updated) VALUES(?,?,1,?)
		ON CONFLICT(scope) DO UPDATE SET reason=excluded.reason,active=1,updated=excluded.updated`, scope, reason, time.Now().UnixMilli()); err != nil {
		return err
	}
	var account int64
	var target string
	switch {
	case strings.HasPrefix(scope, "account:"):
		if _, err := fmt.Sscanf(scope, "account:%d", &account); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE copy_live SET enabled=0 WHERE account_id=?", account); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE virtual_sessions SET active=0 WHERE account_id=?", account); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE copy_tasks SET state='cancelled',error=? WHERE account_id=? AND state='pending'", reason, account); err != nil {
			return err
		}
	case strings.HasPrefix(scope, "target:"):
		target = strings.TrimPrefix(scope, "target:")
		if _, err := tx.ExecContext(ctx, "UPDATE copy_live SET enabled=0 WHERE target_id=?", target); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE virtual_sessions SET active=0 WHERE target_id=?", target); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE copy_tasks SET state='cancelled',error=? WHERE target_id=? AND state='pending'", reason, target); err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid breaker scope %s", scope)
	}
	_, err := s.RaiseAlert(ctx, tx, store.AlertSpec{Key: "breaker:" + scope, Scope: scope, Severity: store.SeverityCritical, Title: "跟单已自动暂停", Message: reason})
	return err
}

// ResetBreaker clears the breaker record and its alert. Copy is not re-enabled
// automatically; the operator must explicitly turn it back on.
// ResetBreaker clears the breaker record and its alert. For an account the
// current reconciliation mismatches are acknowledged, so the operator's manual
// review is final: the same mismatch will not pause copy again. A later change
// in the local attribution is a new mismatch and is evaluated normally.
func (s *Service) ResetBreaker(ctx context.Context, scope string) error {
	if !strings.HasPrefix(scope, "account:") && !strings.HasPrefix(scope, "target:") {
		return store.InputError("风控范围无效")
	}
	if err := s.ResetBreakerRecord(ctx, scope); err != nil {
		return err
	}
	if strings.HasPrefix(scope, "account:") {
		var account int64
		if _, err := fmt.Sscanf(scope, "account:%d", &account); err == nil {
			s.acknowledgeReconciliations(ctx, account)
		}
	}
	s.resolve(ctx, "breaker:"+scope, "风控暂停已人工解除")
	return nil
}

// acknowledgeReconciliations records that the operator reviewed the current
// mismatches and clears their alerts.
func (s *Service) acknowledgeReconciliations(ctx context.Context, account int64) {
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		log.Printf("确认对账记录失败 account=%d: %v", account, err)
		return
	}
	defer tx.Rollback()
	if _, err = s.AcknowledgeReconciliation(ctx, tx, account); err != nil {
		log.Printf("确认对账记录失败 account=%d: %v", account, err)
		return
	}
	rows, err := tx.QueryContext(ctx, "SELECT symbol,position_side FROM position_reconciliation WHERE account_id=? AND acknowledged=1", account)
	if err != nil {
		log.Printf("确认对账记录失败 account=%d: %v", account, err)
		return
	}
	type entry struct{ symbol, side string }
	var entries []entry
	for rows.Next() {
		var e entry
		if err = rows.Scan(&e.symbol, &e.side); err != nil {
			rows.Close()
			return
		}
		entries = append(entries, e)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return
	}
	rows.Close()
	for _, e := range entries {
		key := fmt.Sprintf("reconcile:%d:%s:%s", account, e.symbol, e.side)
		if _, err = s.ResolveAlert(ctx, tx, key, "已人工核对对账差异，不再重复提醒"); err != nil {
			return
		}
	}
	if err = tx.Commit(); err != nil {
		log.Printf("确认对账记录失败 account=%d: %v", account, err)
	}
}

func (s *Service) ResetBreakerRecord(ctx context.Context, scope string) error {
	return s.SQLiteStore.ResetBreaker(ctx, scope)
}

func (s *Service) runReconcile(ctx context.Context, client *http.Client) {
	accounts, err := s.accounts(ctx)
	if err != nil {
		return
	}
	for _, account := range accounts {
		if err := s.reconcileAccount(ctx, client, account.ID, account.Key, account.Secret); err != nil {
			log.Printf("持仓对账失败 account=%d: %v", account.ID, err)
			s.raise(ctx, store.AlertSpec{Key: fmt.Sprintf("reconcile:%d:api", account.ID), Scope: fmt.Sprintf("account:%d", account.ID), Severity: store.SeverityWarning, Title: "持仓对账失败", Message: fmt.Sprintf("账户 %s 持仓对账失败：%v", account.Name, err)})
		}
	}
}

func (s *Service) reconcileAccount(ctx context.Context, client *http.Client, account int64, key, secret string) error {
	c, err := exchange.NewFuturesClient(client, "https://fapi.binance.com", key, secret)
	if err != nil {
		return err
	}
	positions, err := c.Positions(ctx)
	if err != nil {
		return err
	}
	if err = s.reconcilePositions(ctx, account, positions); err != nil {
		return err
	}
	s.resolve(ctx, fmt.Sprintf("reconcile:%d:api", account), "持仓对账恢复")
	return nil
}

func (s *Service) reconcilePositions(ctx context.Context, account int64, positions []exchange.FuturesPosition) error {
	local := map[string]*big.Rat{}
	rows, err := s.DB().QueryContext(ctx, "SELECT symbol,position_side,quantity FROM copied_positions WHERE account_id=?", account)
	if err != nil {
		return err
	}
	for rows.Next() {
		var symbol, side, quantity string
		if err = rows.Scan(&symbol, &side, &quantity); err != nil {
			rows.Close()
			return err
		}
		value, err := decimal(quantity)
		if err != nil {
			rows.Close()
			return err
		}
		if value.Sign() == 0 {
			continue
		}
		local[symbol+"|"+side] = value
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	actualKeys := map[string]bool{}
	for _, p := range positions {
		if p.Symbol == "" {
			continue
		}
		amount, err := signedDecimal(p.Amount)
		if err != nil || amount.Sign() == 0 {
			continue
		}
		side := p.Side
		if side == "BOTH" {
			if amount.Sign() > 0 {
				side = "LONG"
			} else {
				side = "SHORT"
			}
		}
		if side == "LONG" || side == "SHORT" {
			actualKeys[p.Symbol+"|"+side] = true
		}
	}
	for key := range local {
		actualKeys[key] = true
	}
	keys := make([]string, 0, len(actualKeys))
	for key := range actualKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	keep := map[string]bool{}
	tripped := false
	for _, mapKey := range keys {
		parts := strings.SplitN(mapKey, "|", 2)
		symbol, side := parts[0], parts[1]
		actualText, err := exchange.PositionAmount(positions, symbol, side)
		if err != nil {
			return err
		}
		actual, err := decimal(actualText)
		if err != nil {
			return err
		}
		localQty := new(big.Rat)
		if value := local[mapKey]; value != nil {
			localQty = value
		}
		difference := new(big.Rat).Sub(localQty, actual)
		status := "match"
		if difference.Sign() > 0 {
			status = "over"
		} else if difference.Sign() < 0 {
			status = "unattributed"
		}
		keep[mapKey] = true
		if err = s.SaveReconciliationWithAck(ctx, tx, account, symbol, side, decimalString(actual), decimalString(localQty), decimalString(difference), status); err != nil {
			return err
		}
		alertKey := fmt.Sprintf("reconcile:%d:%s:%s", account, symbol, side)
		if status == "over" {
			var acknowledged bool
			if err = tx.QueryRowContext(ctx, "SELECT acknowledged FROM position_reconciliation WHERE account_id=? AND symbol=? AND position_side=?", account, symbol, side).Scan(&acknowledged); err != nil {
				return err
			}
			if acknowledged {
				// The operator reviewed this same mismatch and resumed copy.
				continue
			}
			if _, err = s.RaiseAlert(ctx, tx, store.AlertSpec{Key: alertKey, Scope: fmt.Sprintf("account:%d", account), Severity: store.SeverityCritical, Title: "持仓对账不一致", Message: fmt.Sprintf("%s %s 本地归属 %s 超过交易所实际 %s", symbol, side, decimalString(localQty), decimalString(actual))}); err != nil {
				return err
			}
			if !tripped {
				if err = s.tripTx(ctx, tx, fmt.Sprintf("account:%d", account), fmt.Sprintf("持仓对账不一致：%s %s 超配", symbol, side)); err != nil {
					return err
				}
				tripped = true
			}
		} else {
			if _, err = s.ResolveAlert(ctx, tx, alertKey, "持仓对账恢复"); err != nil {
				return err
			}
		}
	}
	if err = s.PruneReconciliation(ctx, tx, account, keep); err != nil {
		return err
	}
	return tx.Commit()
}

// Metrics renders a small Prometheus text exposition without extra deps.
func (s *Service) Metrics(ctx context.Context) (string, error) {
	var builder strings.Builder
	gauge := func(name, help string, value any, labels string) {
		fmt.Fprintf(&builder, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)
		fmt.Fprintf(&builder, "%s%s %v\n", name, labels, value)
	}
	builder.WriteString("# HELP order_monitor_up Process is running\n# TYPE order_monitor_up gauge\norder_monitor_up 1\n")
	var enabled, pendingOutbox, unresolved, breakers, unhealthy, over int
	if err := s.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM targets WHERE enabled=1").Scan(&enabled); err != nil {
		return "", err
	}
	if err := s.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox WHERE status='pending'").Scan(&pendingOutbox); err != nil {
		return "", err
	}
	if err := s.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM copy_tasks WHERE state='submitted'").Scan(&unresolved); err != nil {
		return "", err
	}
	if err := s.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM risk_breakers WHERE active=1").Scan(&breakers); err != nil {
		return "", err
	}
	if err := s.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM account_health WHERE ok=0").Scan(&unhealthy); err != nil {
		return "", err
	}
	if err := s.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM position_reconciliation WHERE status='over'").Scan(&over); err != nil {
		return "", err
	}
	gauge("order_monitor_targets_enabled", "Enabled leader targets", enabled, "")
	gauge("order_monitor_notifications_pending", "Pending notification deliveries", pendingOutbox, "")
	gauge("order_monitor_unresolved_orders", "Copy tasks awaiting confirmation", unresolved, "")
	gauge("order_monitor_risk_breakers_active", "Active risk breakers", breakers, "")
	gauge("order_monitor_accounts_unhealthy", "Accounts failing API key health checks", unhealthy, "")
	gauge("order_monitor_reconciliation_over_attributed", "Positions locally over-attributed", over, "")
	rows, err := s.DB().QueryContext(ctx, "SELECT state,COUNT(*) FROM copy_tasks GROUP BY state")
	if err != nil {
		return "", err
	}
	builder.WriteString("# HELP order_monitor_copy_tasks Copy tasks by state\n# TYPE order_monitor_copy_tasks gauge\n")
	for rows.Next() {
		var state string
		var count int
		if err = rows.Scan(&state, &count); err != nil {
			rows.Close()
			return "", err
		}
		fmt.Fprintf(&builder, "order_monitor_copy_tasks{state=%q} %d\n", state, count)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()
	rows, err = s.DB().QueryContext(ctx, "SELECT severity,COUNT(*) FROM alerts WHERE status='active' GROUP BY severity")
	if err != nil {
		return "", err
	}
	builder.WriteString("# HELP order_monitor_active_alerts Active alerts by severity\n# TYPE order_monitor_active_alerts gauge\n")
	for rows.Next() {
		var severity string
		var count int
		if err = rows.Scan(&severity, &count); err != nil {
			rows.Close()
			return "", err
		}
		fmt.Fprintf(&builder, "order_monitor_active_alerts{severity=%q} %d\n", severity, count)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()
	return builder.String(), nil
}

// SaveAccountChecked validates credentials before saving and records health.
func (s *Service) SaveAccountChecked(ctx context.Context, client *http.Client, in store.AccountInput) error {
	var status exchange.AccountStatus
	checked := false
	key := strings.TrimSpace(in.Key)
	secret := strings.TrimSpace(in.Secret)
	if key != "" {
		var err error
		status, err = exchange.CheckAccount(ctx, client, key, secret)
		if err != nil {
			return store.InputError("API Key 验证失败：" + err.Error())
		}
		if !status.CanTrade {
			return store.InputError("API Key 没有合约交易权限")
		}
		checked = true
	}
	if err := s.SaveAccount(ctx, in); err != nil {
		return err
	}
	if !checked {
		return nil
	}
	var id int64
	if in.ID != nil {
		id = *in.ID
	} else if err := s.DB().QueryRowContext(ctx, "SELECT id FROM futures_accounts WHERE name=?", in.Name).Scan(&id); err != nil {
		return err
	}
	if _, err := s.SaveAccountHealth(ctx, id, true, status.CanTrade, status.CanWithdraw, ""); err != nil {
		return err
	}
	s.resolve(ctx, fmt.Sprintf("account-health:%d", id), "API Key 已更新并验证通过")
	return nil
}
