package copy

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"order-monitor/internal/exchange"
	"strings"
	"time"
)

func (s *Service) preparePending(ctx context.Context, task PendingCopy, c *FuturesClient, leader *exchange.LeaderClient) (PreparedCopy, error) {
	var prepared PreparedCopy
	obs, err := task.Body.Observation(task.Target)
	now := time.Now()
	if err != nil || obs.Key.UpdateTime < now.Add(-3*time.Minute).UnixMilli() || obs.Key.UpdateTime > now.Add(5*time.Second).UnixMilli() {
		return prepared, errors.New("带单员交易操作超过 3 分钟，取消跟单")
	}
	if task.Action == "open" && task.Retries > 0 {
		var closed bool
		err = s.DB().QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM virtual_events WHERE account_id=? AND target_id=? AND symbol=? AND position_side=? AND stamp>=? AND action IN ('close','unmatched_close') AND order_key!=?)`, task.AccountID, task.Target, obs.Key.Symbol, obs.Key.PositionSide, obs.Key.UpdateTime, task.OriginalKey).Scan(&closed)
		if err != nil {
			return prepared, err
		}
		if closed {
			return prepared, errors.New("带单员后续已平仓或减仓，取消重试开仓")
		}
	}
	in := PlanInput{Key: obs.Key, Action: task.Action, Signal: Signal{Quantity: task.Body.Quantity.String(), Price: task.Body.Price.String()}, LeaderBefore: task.Before, LeaderBalance: task.LeaderBalance, Multiplier: task.Multiplier}
	if in.LeaderBalance == "" {
		detail, err := leader.Detail(ctx, task.Target)
		if err != nil {
			return prepared, fmt.Errorf("%w: %v", exchange.ErrNetwork, err)
		}
		in.LeaderBalance = detail.MarginBalance.String()
		if _, err = positiveDecimal(in.LeaderBalance, "带单员余额"); err != nil {
			return prepared, err
		}
		if _, err = s.DB().ExecContext(ctx, "UPDATE copy_tasks SET lead_balance=? WHERE id=? AND state='pending'", in.LeaderBalance, task.ID); err != nil {
			return prepared, err
		}
	}
	prepared.LeaderBalance = in.LeaderBalance
	balances, err := c.Balances(ctx)
	if err != nil {
		return prepared, err
	}
	prepared.BalanceAt = time.Now().UnixMilli()
	for _, b := range balances {
		if b.Asset == "USDT" {
			in.WalletBalance, in.Available = b.Balance, b.Available
			prepared.Wallet, prepared.Available = &in.WalletBalance, &in.Available
			break
		}
	}
	positions, err := c.Positions(ctx)
	if err != nil {
		return prepared, err
	}
	if in.HedgeMode, err = c.HedgeMode(ctx); err != nil {
		return prepared, err
	}
	rules, err := c.MarketRules(ctx)
	if err != nil {
		return prepared, err
	}
	if in.Market, err = c.Market(ctx, in.Key.Symbol, rules, task.Action == "open"); err != nil {
		return prepared, err
	}
	if task.Action == "open" {
		opposite := "SHORT"
		if in.Key.PositionSide == "SHORT" {
			opposite = "LONG"
		}
		if in.OppositePosition, err = PositionAmount(positions, in.Key.Symbol, opposite); err != nil {
			return prepared, err
		}
		var configs []struct {
			Symbol   string
			Leverage json.Number
		}
		if err = c.Request(ctx, http.MethodGet, "/fapi/v1/symbolConfig", url.Values{"symbol": {in.Key.Symbol}}, true, &configs); err != nil {
			return prepared, err
		}
		for _, v := range configs {
			if v.Symbol == in.Key.Symbol {
				in.Leverage = v.Leverage.String()
				break
			}
		}
		if task.Body.Incremental && !task.Body.Standalone {
			if err = s.planIncrement(ctx, task, &in); err != nil {
				return prepared, err
			}
		}
	} else {
		if in.ActualPosition, err = PositionAmount(positions, in.Key.Symbol, in.Key.PositionSide); err != nil {
			return prepared, err
		}
		prepared.Close, err = s.prepareReduction(ctx, task, &in)
		if err != nil {
			return prepared, err
		}
	}
	prepared.ReferencePrice = in.Market.Bid
	if in.Key.Side == "BUY" {
		prepared.ReferencePrice = in.Market.Ask
	}
	prepared.Order, err = PlanOrder(in)
	return prepared, err
}

func (s *Service) planIncrement(ctx context.Context, t PendingCopy, in *PlanInput) error {
	fill, err := s.Load(ctx, in.Key)
	if err != nil {
		return err
	}
	if fill == (CumulativeFill{}) {
		var raw string
		base, _, _ := strings.Cut(t.OriginalKey, ":")
		err = s.DB().QueryRowContext(ctx, "SELECT body FROM records WHERE target=? AND kind='order' AND key=?", t.Target, base).Scan(&raw)
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("缺少累计成交记录，无法计算增量")
		}
		if err != nil {
			return err
		}
		var row LeaderFill
		if err = json.Unmarshal([]byte(raw), &row); err != nil {
			return err
		}
		original, err := row.Observation(t.Target)
		if err != nil || original.Key != in.Key {
			return errors.New("累计成交记录不匹配")
		}
		in.Cumulative = &Signal{Quantity: row.Quantity.String(), Price: row.Price.String()}
	} else {
		q, err := positiveDecimal(fill.Quantity, "累计成交量")
		if err != nil {
			return err
		}
		cost, err := positiveDecimal(fill.Cost, "累计成交额")
		if err != nil {
			return err
		}
		in.Cumulative = &Signal{Quantity: fill.Quantity, Price: new(big.Rat).Quo(cost, q).FloatString(40)}
	}
	base, _, _ := strings.Cut(t.OriginalKey, ":")
	rows, err := s.DB().QueryContext(ctx, `SELECT json_extract(result,'$.executedQty') FROM copy_tasks WHERE account_id=? AND target_id=? AND id<? AND (order_key=? OR order_key LIKE ?) AND state IN ('filled','finished')`, t.AccountID, t.Target, t.ID, base, base+":%")
	if err != nil {
		return err
	}
	defer rows.Close()
	total := new(big.Rat)
	for rows.Next() {
		var raw string
		if err = rows.Scan(&raw); err != nil {
			return err
		}
		q, err := decimal(raw)
		if err != nil {
			return err
		}
		total.Add(total, q)
	}
	in.AlreadyFilled = decimalString(total)
	return rows.Err()
}

func (s *Service) executeNextPending(ctx context.Context, httpClient *http.Client, leader *exchange.LeaderClient) (bool, error) {
	task, ok, err := s.NextPending(ctx, time.Now())
	if err != nil || !ok {
		return ok, err
	}
	c, err := exchange.NewFuturesClient(httpClient, "https://fapi.binance.com", task.APIKey, task.APISecret)
	if err != nil {
		return false, err
	}
	plan, err := s.preparePending(ctx, task, c, leader)
	if err == nil {
		return true, s.SubmitPending(ctx, task, plan, c)
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	tx, dbErr := s.DB().BeginTx(ctx, nil)
	if dbErr != nil {
		return false, dbErr
	}
	defer tx.Rollback()
	var exchangeErr ExchangeError
	transient := errors.Is(err, exchange.ErrNetwork)
	if errors.As(err, &exchangeErr) {
		status := exchangeErr.Status
		transient = status == 408 || status == 418 || status == 429 || status == 451 || status >= 500 && status <= 599
	}
	if transient && task.Retries < 3 {
		retry := task.Retries + 1
		_, dbErr = tx.ExecContext(ctx, "UPDATE copy_tasks SET retry_count=?,next_check=?,error=? WHERE id=? AND state='pending'", retry, time.Now().Unix()+int64(5<<task.Retries), fmt.Sprintf("预检查网络错误，等待第 %d/3 次重试：%v", retry, err), task.ID)
		if dbErr != nil {
			return false, dbErr
		}
		return true, tx.Commit()
	}
	reason := err.Error()
	if transient {
		reason = "预检查重试 3 次后失败：" + reason
	}
	if errors.Is(err, errNoRemainingPosition) {
		var live bool
		dbErr = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM copy_live l JOIN copy_bindings b ON b.account_id=l.account_id AND b.target_id=l.target_id JOIN targets t ON t.id=l.target_id JOIN copy_tasks c ON c.account_id=l.account_id AND c.target_id=l.target_id WHERE c.id=? AND c.state='pending' AND l.enabled=1 AND t.enabled=1)`, task.ID).Scan(&live)
		if dbErr != nil {
			return false, dbErr
		}
		if live {
			_, dbErr = tx.ExecContext(ctx, "UPDATE copied_positions SET quantity='0' WHERE account_id=? AND target_id=? AND symbol=? AND position_side=? AND quantity=?", task.AccountID, task.Target, task.Body.Symbol, task.Body.PositionSide, plan.Close.Tracked)
			if dbErr != nil {
				return false, dbErr
			}
		} else {
			reason = "跟单关联或开关状态不一致，未调整持仓记录"
		}
	}
	res, dbErr := tx.ExecContext(ctx, "UPDATE copy_tasks SET state='skipped',error=? WHERE id=? AND state='pending'", reason, task.ID)
	if dbErr != nil {
		return false, dbErr
	}
	n, dbErr := res.RowsAffected()
	if dbErr != nil {
		return false, dbErr
	}
	if n != 0 {
		dbErr = copyNotice(ctx, tx, task.Target, map[string]any{"copy_task_id": task.ID, "message": task.Body.Symbol + " 跟单跳过：" + reason})
		if dbErr != nil {
			return false, dbErr
		}
	}
	return true, tx.Commit()
}
