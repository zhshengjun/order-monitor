package copy

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"order-monitor/internal/store"
)

func copyNotice(ctx context.Context, tx *sql.Tx, target string, body map[string]any) error {
	severity := store.NormalizeSeverity(fmt.Sprint(body["severity"]), store.SeverityWarning)
	body["severity"] = severity
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	event, err := tx.ExecContext(ctx, `INSERT INTO events(target,name,kind,body,stamp,historical) VALUES(?,COALESCE((SELECT name FROM targets WHERE id=?),?),'copy',?,?,0)`, target, target, target, string(data), time.Now().UnixMilli())
	if err != nil {
		return err
	}
	id, err := event.LastInsertId()
	if err != nil {
		return err
	}
	return store.QueueEventSeverity(ctx, tx, id, severity)
}
