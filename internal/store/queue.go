package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

func queueEvent(ctx context.Context, tx *sql.Tx, event int64) error {
	var raw string
	err := tx.QueryRowContext(ctx, "SELECT value FROM settings WHERE id=1").Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var settings map[string]any
	if err = json.Unmarshal([]byte(raw), &settings); err != nil {
		return err
	}
	for _, channel := range []string{"telegram", "bark"} {
		if settings[channel+"_enabled"] == true {
			if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO outbox(event,channel) VALUES(?,?)", event, channel); err != nil {
				return err
			}
		}
	}
	return nil
}
