package store

import (
	"context"
	"database/sql"
	"time"
)

// SetVirtualTrust records whether the virtual leader position for a binding can
// be used for automatic full-close decisions.
func SetVirtualTrust(ctx context.Context, tx *sql.Tx, account int64, target string, trusted bool, reason string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO virtual_trust(account_id,target_id,trusted,reason,updated) VALUES(?,?,?,?,?)
	ON CONFLICT(account_id,target_id) DO UPDATE SET trusted=excluded.trusted,reason=excluded.reason,updated=excluded.updated`,
		account, target, trusted, reason, time.Now().UnixMilli())
	return err
}
