package store

import (
	"context"
	"database/sql"
	"time"
)

func (s *SQLiteStore) SaveAccountHealth(ctx context.Context, account int64, status bool, canTrade, canWithdraw bool, message string) (int, error) {
	failures := 0
	if !status {
		failures = 1
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO account_health(account_id,ok,can_trade,can_withdraw,checked_at,error,failures) VALUES(?,?,?,?,?,?,?)
	ON CONFLICT(account_id) DO UPDATE SET ok=excluded.ok,can_trade=excluded.can_trade,can_withdraw=excluded.can_withdraw,checked_at=excluded.checked_at,error=excluded.error,
	failures=CASE WHEN excluded.ok=1 THEN 0 ELSE account_health.failures+1 END`,
		account, status, canTrade, canWithdraw, time.Now().UnixMilli(), message, failures)
	if err != nil {
		return 0, err
	}
	err = s.db.QueryRowContext(ctx, "SELECT failures FROM account_health WHERE account_id=?", account).Scan(&failures)
	return failures, err
}

func (s *SQLiteStore) ActiveBreaker(ctx context.Context, scope string) (bool, error) {
	var active bool
	err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM risk_breakers WHERE scope=? AND active=1)", scope).Scan(&active)
	return active, err
}

func (s *SQLiteStore) ResetBreaker(ctx context.Context, scope string) error {
	res, err := s.db.ExecContext(ctx, "UPDATE risk_breakers SET active=0,updated=? WHERE scope=? AND active=1", time.Now().UnixMilli(), scope)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return InputError("风控暂停记录不存在或已恢复")
	}
	return nil
}

func (s *SQLiteStore) SaveReconciliation(ctx context.Context, tx *sql.Tx, account int64, symbol, side, actual, local, difference, status string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO position_reconciliation(account_id,symbol,position_side,actual_quantity,local_quantity,difference,status,checked_at)
	VALUES(?,?,?,?,?,?,?,?)
	ON CONFLICT(account_id,symbol,position_side) DO UPDATE SET actual_quantity=excluded.actual_quantity,local_quantity=excluded.local_quantity,difference=excluded.difference,status=excluded.status,checked_at=excluded.checked_at`,
		account, symbol, side, actual, local, difference, status, time.Now().UnixMilli())
	return err
}

// ResolveReconciliation removes rows for positions that no longer exist.
func (s *SQLiteStore) PruneReconciliation(ctx context.Context, tx *sql.Tx, account int64, keep map[string]bool) error {
	rows, err := tx.QueryContext(ctx, "SELECT symbol,position_side FROM position_reconciliation WHERE account_id=?", account)
	if err != nil {
		return err
	}
	var stale [][2]string
	for rows.Next() {
		var symbol, side string
		if err = rows.Scan(&symbol, &side); err != nil {
			rows.Close()
			return err
		}
		if !keep[symbol+"|"+side] {
			stale = append(stale, [2]string{symbol, side})
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, key := range stale {
		if _, err = tx.ExecContext(ctx, "DELETE FROM position_reconciliation WHERE account_id=? AND symbol=? AND position_side=?", account, key[0], key[1]); err != nil {
			return err
		}
	}
	return nil
}
