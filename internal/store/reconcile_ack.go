package store

import (
	"context"
	"database/sql"
	"time"
)

// AcknowledgeReconciliation marks every local over-attribution of an account
// as reviewed by the operator. Acknowledged positions no longer raise alerts or
// trip the breaker; they are only re-evaluated when the local quantity changes.
func (s *SQLiteStore) AcknowledgeReconciliation(ctx context.Context, tx *sql.Tx, account int64) (int, error) {
	res, err := tx.ExecContext(ctx, "UPDATE position_reconciliation SET acknowledged=1 WHERE account_id=? AND acknowledged=0", account)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// SaveReconciliation records one position comparison. acknowledged is preserved
// unless the local attribution changed since the acknowledgement.
func (s *SQLiteStore) SaveReconciliationWithAck(ctx context.Context, tx *sql.Tx, account int64, symbol, side, actual, local, difference, status string) error {
	var previousLocal string
	var acknowledged bool
	err := tx.QueryRowContext(ctx, "SELECT local_quantity,acknowledged FROM position_reconciliation WHERE account_id=? AND symbol=? AND position_side=?", account, symbol, side).Scan(&previousLocal, &acknowledged)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == sql.ErrNoRows {
		previousLocal, acknowledged = "", false
	}
	// A changed local attribution is a new mismatch that needs a new decision.
	// A match clears the acknowledgement entirely.
	if status == "match" || acknowledged && previousLocal != local {
		acknowledged = false
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO position_reconciliation(account_id,symbol,position_side,actual_quantity,local_quantity,difference,status,checked_at,acknowledged)
	VALUES(?,?,?,?,?,?,?,?,?)
	ON CONFLICT(account_id,symbol,position_side) DO UPDATE SET actual_quantity=excluded.actual_quantity,local_quantity=excluded.local_quantity,
	difference=excluded.difference,status=excluded.status,checked_at=excluded.checked_at,acknowledged=excluded.acknowledged`,
		account, symbol, side, actual, local, difference, status, time.Now().UnixMilli(), acknowledged)
	return err
}

// AcknowledgedOverAttributions lists positions the operator already accepted.
func (s *SQLiteStore) AcknowledgedOverAttributions(ctx context.Context, account int64) (map[string]bool, error) {
	rows, err := s.DB().QueryContext(ctx, "SELECT symbol,position_side FROM position_reconciliation WHERE account_id=? AND acknowledged=1", account)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]bool{}
	for rows.Next() {
		var symbol, side string
		if err = rows.Scan(&symbol, &side); err != nil {
			return nil, err
		}
		result[symbol+"|"+side] = true
	}
	return result, rows.Err()
}
