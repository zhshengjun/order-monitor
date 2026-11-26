package engine

import (
	"context"
	"fmt"
)

func (s *SQLiteStore) stopCopyForRegression(ctx context.Context, observation Observation) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	target := observation.Key.Portfolio
	for _, query := range []string{
		"UPDATE copy_live SET enabled=0 WHERE target_id=? AND enabled=1",
		"UPDATE virtual_sessions SET active=0 WHERE target_id=? AND account_id IN (SELECT account_id FROM copy_live WHERE target_id=?)",
	} {
		if query == "UPDATE virtual_sessions SET active=0 WHERE target_id=? AND account_id IN (SELECT account_id FROM copy_live WHERE target_id=?)" {
			if _, err = tx.ExecContext(ctx, query, target, target); err != nil {
				return err
			}
		} else if _, err = tx.ExecContext(ctx, query, target); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE copy_tasks SET state='cancelled',error='订单成交数据异常' WHERE target_id=? AND state='pending'", target); err != nil {
		return err
	}
	message := fmt.Sprintf("%s 订单 %d 的累计成交数量或成本异常，已关闭跟单，请核对记录", observation.Key.Symbol, observation.Key.UpdateTime)
	if err = copyNotice(ctx, tx, target, map[string]any{"message": message}); err != nil {
		return err
	}
	return tx.Commit()
}
