package copy

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"order-monitor/internal/exchange"
)

// SettleCopy records the confirmed execution and its account/leader position
// in one transaction. Repeated responses for a completed task are harmless.
func (s *Service) SettleCopy(ctx context.Context, taskID int64, result OrderResult) error {
	state := "finished"
	switch result.Status {
	case "FILLED":
		state = "filled"
	case "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH":
	default:
		return errors.New("order is not terminal")
	}
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var account int64
	var target, action, clientID, status, body string
	var quantity sql.NullString
	if err = tx.QueryRowContext(ctx, "SELECT account_id,target_id,action,client_id,state,body,quantity FROM copy_tasks WHERE id=?", taskID).Scan(&account, &target, &action, &clientID, &status, &body, &quantity); err != nil {
		return err
	}
	if status != "submitted" {
		return nil
	}
	var signal struct {
		Symbol                      string
		PositionSide                string
		Side, BaseAsset, QuoteAsset string
	}
	if err = json.Unmarshal([]byte(body), &signal); err != nil {
		return err
	}
	requested, err := positiveDecimal(quantity.String, "submitted quantity")
	if err != nil {
		return err
	}
	filled, err := decimal(result.Executed)
	if err != nil || filled.Cmp(requested) > 0 || result.ClientID != "" && result.ClientID != clientID || result.Symbol != "" && result.Symbol != signal.Symbol || result.Side != "" && result.Side != signal.Side {
		return errors.New("execution does not match submitted order")
	}
	var owned string
	err = tx.QueryRowContext(ctx, "SELECT quantity FROM copied_positions WHERE account_id=? AND target_id=? AND symbol=? AND position_side=?", account, target, signal.Symbol, signal.PositionSide).Scan(&owned)
	if errors.Is(err, sql.ErrNoRows) {
		owned = "0"
	} else if err != nil {
		return err
	}
	before, err := decimal(owned)
	if err != nil {
		return err
	}
	fullClose := false
	after := new(big.Rat)
	if action == "open" {
		after.Add(before, filled)
	} else {
		var tracked, actual string
		err = tx.QueryRowContext(ctx, "SELECT tracked_quantity,actual_quantity FROM copy_close_snapshots WHERE task_id=? AND leader_full_close=1", taskID).Scan(&tracked, &actual)
		if err == nil {
			fullClose = true
			original, err := decimal(tracked)
			if err != nil {
				return err
			}
			available, err := decimal(actual)
			if err != nil {
				return err
			}
			if filled.Sign() > 0 && before.Cmp(original) == 0 && available.Cmp(before) < 0 {
				before = available
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		after.Sub(before, filled)
		if after.Sign() < 0 {
			after.SetInt64(0)
		}
	}
	if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO copied_positions VALUES(?,?,?,?,?)", account, target, signal.Symbol, signal.PositionSide, decimalString(after)); err != nil {
		return err
	}
	if action != "open" && retryableClose(result.Status) && fullClose && filled.Cmp(requested) < 0 && after.Sign() > 0 {
		if err = s.enqueueResidualClose(ctx, tx, taskID, account, target, after); err != nil {
			return err
		}
	}
	notional := exchange.ExecutionNotional(result, filled)
	var quote any
	var avg *big.Rat
	if notional != nil {
		quote = decimalString(notional)
		if filled.Sign() > 0 {
			avg = new(big.Rat).Quo(notional, filled)
		}
	}
	encoded, err := json.Marshal(map[string]any{"status": result.Status, "executedQty": decimalString(filled), "orderId": result.OrderID, "cumQuote": quote})
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE copy_tasks SET state=?,result=?,error='',reconcile_attempts=0 WHERE id=? AND state='submitted'", state, string(encoded), taskID); err != nil {
		return err
	}
	direction := map[[2]string]string{{"BUY", "LONG"}: "开多", {"SELL", "LONG"}: "平多", {"SELL", "SHORT"}: "开空", {"BUY", "SHORT"}: "平空"}[[2]string{signal.Side, signal.PositionSide}]
	message := fmt.Sprintf("%s %s，状态 %s\n实际成交数量：%s %s\n实际成交均价：%s %s", signal.Symbol, direction, result.Status, noticeNumber(filled), signal.BaseAsset, noticeNumber(avg), signal.QuoteAsset)
	if err := copyNotice(ctx, tx, target, map[string]any{"message": message, "copy_task_id": taskID, "margin_lookup": true, "margin_action": action}); err != nil {
		return err
	}
	return tx.Commit()
}

// enqueueResidualClose queues one follow-up close when a full-close order only
// partially filled. Three attempts bound the loop; further residuals stay
// visible through copy notices and close_watch.
func retryableClose(status string) bool {
	switch status {
	case "EXPIRED", "CANCELED", "EXPIRED_IN_MATCH":
		return true
	}
	return false
}

func (s *Service) enqueueResidualClose(ctx context.Context, tx *sql.Tx, taskID, account int64, target string, remaining *big.Rat) error {
	var live bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM copy_live l
		JOIN virtual_sessions v ON v.account_id=l.account_id AND v.target_id=l.target_id AND v.active=1
		JOIN copy_bindings b ON b.account_id=l.account_id AND b.target_id=l.target_id
		JOIN targets t ON t.id=l.target_id AND t.enabled=1
		WHERE l.account_id=? AND l.target_id=? AND l.enabled=1)`, account, target).Scan(&live); err != nil {
		return err
	}
	if !live {
		return copyNotice(ctx, tx, target, map[string]any{"message": fmt.Sprintf("平仓未完全成交，剩余 %s 未平；跟单已关闭，请人工处理", decimalString(remaining))})
	}
	var orderKey, before, leadBalance, body string
	var multiplier string
	err := tx.QueryRowContext(ctx, `SELECT c.order_key,c.before_qty,c.lead_balance,c.body,COALESCE(m.multiplier,'1')
	FROM copy_tasks c LEFT JOIN copy_task_multiplier_v2 m ON m.task_id=c.id WHERE c.id=?`, taskID).Scan(&orderKey, &before, &leadBalance, &body, &multiplier)
	if err != nil {
		return err
	}
	base := orderKey
	if i := strings.Index(base, ":rp"); i >= 0 {
		base = base[:i]
	}
	var attempts int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM copy_tasks WHERE account_id=? AND target_id=? AND order_key LIKE ?", account, target, base+":rp%").Scan(&attempts); err != nil {
		return err
	}
	if attempts >= 3 {
		if err = copyNotice(ctx, tx, target, map[string]any{"message": fmt.Sprintf("平仓未完全成交，剩余 %s 未平；已重试 %d 次，请人工核对", decimalString(remaining), attempts)}); err != nil {
			return err
		}
		return nil
	}
	key := fmt.Sprintf("%s:rp%d", base, attempts+1)
	hash := sha256.Sum256([]byte(fmt.Sprintf("%d:%s:%s", account, target, key)))
	clientID := "cp_" + hex.EncodeToString(hash[:15])
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,state,created)
	VALUES(?,?,?,?,'close',?,?,?,'pending',?)`, account, target, key, body, before, leadBalance, clientID, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 1 {
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO copy_task_multiplier_v2 VALUES(?,?)", id, multiplier); err != nil {
			return err
		}
		if err = copyNotice(ctx, tx, target, map[string]any{"message": fmt.Sprintf("平仓未完全成交，剩余 %s 已重新排队（第 %d 次）", decimalString(remaining), attempts+1)}); err != nil {
			return err
		}
	}
	return nil
}
