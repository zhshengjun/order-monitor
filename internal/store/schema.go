package store

import (
	"context"
	_ "embed"
)

//go:embed schema.sql
var monitorSchema string

// InitializeMonitorTables creates the tables shared by the monitoring UI and
// copy worker. Existing databases retain their rows.
func (s *SQLiteStore) InitializeMonitorTables(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, monitorSchema); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, field := range []struct{ table, column, declaration string }{
		{"copy_tasks", "retry_count", "INTEGER NOT NULL DEFAULT 0"},
		{"copy_tasks", "reconcile_attempts", "INTEGER NOT NULL DEFAULT 0"},
		{"account_health", "failures", "INTEGER NOT NULL DEFAULT 0"},
		{"manual_closes", "unknown_notified", "INTEGER NOT NULL DEFAULT 0"},
		{"leader_history_sync", "last_batch", "TEXT NOT NULL DEFAULT ''"},
		{"leader_history_sync", "started_at", "INTEGER NOT NULL DEFAULT 0"},
		{"leader_history_sync", "failures", "INTEGER NOT NULL DEFAULT 0"},
		{"position_reconciliation", "acknowledged", "INTEGER NOT NULL DEFAULT 0"},
		{"targets", "balance_time", "INTEGER NOT NULL DEFAULT 0"},
	} {
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?) WHERE name=?)", field.table, field.column).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			if _, err := tx.ExecContext(ctx, "ALTER TABLE "+field.table+" ADD COLUMN "+field.column+" "+field.declaration); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}
