package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
)

func (s *SQLiteStore) prepareReduction(ctx context.Context, task PendingCopy, in *PlanInput) (*CloseSnapshot, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if !task.Body.Incremental {
		var complete, ambiguous bool
		err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM leader_history_sync WHERE target=? AND complete=1),EXISTS(SELECT 1 FROM leader_history WHERE target=? AND stamp=? AND json_extract(body,'$.symbol')=? AND json_extract(body,'$.positionSide')=? AND json_extract(body,'$.side')<>?)`, task.Target, task.Target, task.Body.Created.String(), task.Body.Symbol, task.Body.PositionSide, task.Body.Side).Scan(&complete, &ambiguous)
		if err != nil {
			return nil, err
		}
		if complete && !ambiguous {
			rows, err := dashboardRows(ctx, tx, "SELECT body FROM leader_history WHERE target=? AND stamp<? ORDER BY stamp,key", task.Target, task.Body.Created.String())
			if err != nil {
				return nil, err
			}
			fills := make([]LeaderFill, 0, len(rows))
			for _, r := range rows {
				var f LeaderFill
				if err = json.Unmarshal([]byte(r["body"].(string)), &f); err != nil {
					return nil, err
				}
				fills = append(fills, f)
			}
			positions, err := replayPositions(fills)
			if err != nil {
				return nil, err
			}
			q, err := positiveDecimal(in.Signal.Quantity, "leader quantity")
			if err != nil {
				return nil, err
			}
			for _, p := range positions {
				if p.Symbol == in.Key.Symbol && p.Side == in.Key.PositionSide && !p.Uncertain && p.Quantity != nil {
					before, err := decimal(*p.Quantity)
					if err != nil {
						return nil, err
					}
					if before.Cmp(q) >= 0 {
						in.Action = "close"
						in.LeaderBefore = *p.Quantity
					}
				}
			}
		}
	}
	if in.Action != "close" {
		return nil, errors.New("无法确定带单员平仓前持仓，无法计算平仓比例，请核对")
	}
	owners, err := dashboardRows(ctx, tx, "SELECT target_id,quantity FROM copied_positions WHERE account_id=? AND symbol=? AND position_side=?", task.AccountID, in.Key.Symbol, in.Key.PositionSide)
	if err != nil {
		return nil, err
	}
	total := new(big.Rat)
	amounts := map[string]*big.Rat{}
	for _, r := range owners {
		q, err := decimal(r["quantity"].(string))
		if err != nil {
			return nil, err
		}
		amounts[r["target_id"].(string)] = q
		total.Add(total, q)
	}
	owner := amounts[task.Target]
	if owner == nil {
		owner = new(big.Rat)
	}
	balances, err := dashboardRows(ctx, tx, `SELECT target_id,json_extract(preview,'$.plan.quantity') AS quantity FROM manual_closes WHERE account_id=? AND symbol=? AND side=? AND state IN ('submitted','needs_review') AND target_id<>''
	UNION ALL SELECT target_id,quantity FROM copy_tasks WHERE account_id=? AND state='submitted' AND action IN ('close','unmatched_close') AND json_extract(body,'$.symbol')=? AND json_extract(body,'$.positionSide')=?`, task.AccountID, in.Key.Symbol, in.Key.PositionSide, task.AccountID, in.Key.Symbol, in.Key.PositionSide)
	if err != nil {
		return nil, err
	}
	reserved := map[string]*big.Rat{}
	for _, r := range balances {
		target := r["target_id"].(string)
		q, err := decimal(fmt.Sprint(r["quantity"]))
		if err != nil {
			return nil, errors.New("待确认平仓数量缺失，请核对")
		}
		if reserved[target] == nil {
			reserved[target] = new(big.Rat)
		}
		reserved[target].Add(reserved[target], q)
	}
	available := new(big.Rat).Set(owner)
	for target, q := range reserved {
		owned := amounts[target]
		if owned == nil || q.Cmp(owned) > 0 {
			return nil, errors.New("待确认平仓预留数量超过对应跟单持仓，请核对")
		}
		if target == task.Target {
			available.Sub(owner, q)
			if available.Sign() <= 0 {
				return nil, errors.New("该带单员的跟单持仓已由待确认平仓预留，请核对")
			}
		}
	}
	actual, err := decimal(in.ActualPosition)
	if err != nil {
		return nil, err
	}
	before, err := positiveDecimal(in.LeaderBefore, "leader position")
	if err != nil {
		return nil, err
	}
	reduction, err := positiveDecimal(in.Signal.Quantity, "leader reduction")
	if err != nil {
		return nil, err
	}
	snap := &CloseSnapshot{Tracked: decimalString(owner), Actual: in.ActualPosition, Full: before.Cmp(reduction) == 0}
	if total.Cmp(actual) > 0 {
		if !snap.Full {
			return nil, errors.New("各带单员跟单持仓合计超过账户实际持仓，已跳过平仓，请核对")
		}
		if q := reserved[task.Target]; q != nil && q.Sign() > 0 {
			return nil, errors.New("该带单员有待确认平仓，请核对后处理全平")
		}
		available.Sub(actual, new(big.Rat).Sub(total, owner))
		if available.Sign() < 0 {
			available.SetInt64(0)
		}
		snap.Actual = decimalString(available)
		if owner.Sign() > 0 && available.Sign() == 0 {
			return snap, errNoRemainingPosition
		}
	}
	in.Attributed = decimalString(available)
	return snap, tx.Commit()
}

var errNoRemainingPosition = errors.New("带单员已全平，扣除其他带单员后无可平数量，已清除该带单员的跟单持仓记录")
