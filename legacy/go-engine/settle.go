package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
)

// SettleCopy records the confirmed execution and its account/leader position
// in one transaction. Repeated responses for a completed task are harmless.
func (s *SQLiteStore) SettleCopy(ctx context.Context, taskID int64, result OrderResult) error {
	state := "finished"
	switch result.Status {
	case "FILLED":
		state = "filled"
	case "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH":
	default:
		return errors.New("order is not terminal")
	}
	tx, err := s.db.BeginTx(ctx, nil)
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
	if err != nil || filled.Cmp(requested) > 0 || result.ClientID != "" && result.ClientID != clientID || result.Symbol != "" && result.Symbol != signal.Symbol {
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
	after := new(big.Rat)
	if action == "open" {
		after.Add(before, filled)
	} else {
		var tracked, actual string
		err = tx.QueryRowContext(ctx, "SELECT tracked_quantity,actual_quantity FROM copy_close_snapshots WHERE task_id=? AND leader_full_close=1", taskID).Scan(&tracked, &actual)
		if err == nil {
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
	notional := executionNotional(result, filled)
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
	if _, err = tx.ExecContext(ctx, "UPDATE copy_tasks SET state=?,result=?,error='' WHERE id=? AND state='submitted'", state, string(encoded), taskID); err != nil {
		return err
	}
	direction := map[[2]string]string{{"BUY", "LONG"}: "开多", {"SELL", "LONG"}: "平多", {"SELL", "SHORT"}: "开空", {"BUY", "SHORT"}: "平空"}[[2]string{signal.Side, signal.PositionSide}]
	message := fmt.Sprintf("%s %s，状态 %s\n实际成交数量：%s %s\n实际成交均价：%s %s", signal.Symbol, direction, result.Status, noticeNumber(filled), signal.BaseAsset, noticeNumber(avg), signal.QuoteAsset)
	if err := copyNotice(ctx, tx, target, map[string]any{"message": message, "copy_task_id": taskID, "margin_lookup": true, "margin_action": action}); err != nil {
		return err
	}
	return tx.Commit()
}
