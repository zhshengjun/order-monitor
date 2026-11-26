package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

func queueEvent(ctx context.Context, tx *sql.Tx, event int64) error {
	return QueueEventSeverity(ctx, tx, event, SeverityInfo)
}

// QueueEventSeverity queues an event only to channels whose configured minimum
// severity is met.
func QueueEventSeverity(ctx context.Context, tx *sql.Tx, event int64, severity string) error {
	severity = NormalizeSeverity(severity, SeverityInfo)
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
		if settings[channel+"_enabled"] != true {
			continue
		}
		minimum := SeverityInfo
		if value, ok := settings[channel+"_min_severity"].(string); ok {
			minimum = NormalizeSeverity(value, SeverityInfo)
		}
		if SeverityRank(severity) < SeverityRank(minimum) {
			continue
		}
		if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO outbox(event,channel) VALUES(?,?)", event, channel); err != nil {
			return err
		}
	}
	return nil
}
