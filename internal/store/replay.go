package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"order-monitor/internal/domain"
)

func (s *SQLiteStore) LeaderPositions(ctx context.Context, target string, historyActive bool) (map[string]any, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	monitor, err := dashboardRows(ctx, tx, "SELECT enabled,error,last_check FROM targets WHERE id=?", target)
	if err != nil {
		return nil, err
	}
	if len(monitor) == 0 {
		return nil, InputError("带单员不存在")
	}
	rows, err := tx.QueryContext(ctx, "SELECT body FROM leader_history WHERE target=? ORDER BY stamp,key", target)
	if err != nil {
		return nil, err
	}
	var fills []domain.LeaderFill
	for rows.Next() {
		var raw string
		var f domain.LeaderFill
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal([]byte(raw), &f)
		}
		if err != nil {
			rows.Close()
			return nil, err
		}
		fills = append(fills, f)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	syncRows, err := dashboardRows(ctx, tx, "SELECT page,complete,total,error,updated,next_check,failures FROM leader_history_sync WHERE target=?", target)
	if err != nil {
		return nil, err
	}
	var sync any
	if len(syncRows) > 0 {
		sync = syncRows[0]
	}
	var staged int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM leader_history_stage WHERE target=?", target).Scan(&staged); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	positions, err := domain.ReplayPositions(fills)
	if err != nil {
		return nil, err
	}
	return map[string]any{"positions": positions, "count": len(fills), "staged": staged, "sync": sync, "monitor": monitor[0], "history_active": historyActive}, nil
}
