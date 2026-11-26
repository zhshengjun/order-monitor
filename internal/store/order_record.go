package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

func saveOrderRecord(ctx context.Context, tx *sql.Tx, change Comparison) error {
	key, err := recordIdentity(change.Record)
	if err != nil {
		return err
	}
	encodedKey, err := json.Marshal(change.Key)
	if err != nil {
		return err
	}
	var legacy string
	err = tx.QueryRowContext(ctx, "SELECT original_key FROM go_legacy_order_keys WHERE order_key=?", string(encodedKey)).Scan(&legacy)
	if err == nil {
		key = legacy
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	stamp, err := strconv.ParseInt(fmt.Sprint(change.Record["orderTime"]), 10, 64)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(change.Record)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO records VALUES(?,'order',?,?,?)", change.Key.Portfolio, key, string(raw), stamp); err != nil {
		return err
	}
	eventBody := make(map[string]any, len(change.Record)+2)
	for k, v := range change.Record {
		eventBody[k] = v
	}
	eventBody["direction"] = direction(change.Record)
	eventBody["updated"] = change.Previous != (CumulativeFill{})
	rawEvent, err := json.Marshal(eventBody)
	if err != nil {
		return err
	}
	if change.Previous != (CumulativeFill{}) {
		rows, err := dashboardRows(ctx, tx, "SELECT id,body FROM events WHERE target=? AND kind='order' AND stamp=?", change.Key.Portfolio, stamp)
		if err != nil {
			return err
		}
		identity, err := recordIdentity(change.Record)
		if err != nil {
			return err
		}
		for _, row := range rows {
			body, err := DecodeRecord(row["body"])
			if err != nil {
				return err
			}
			other, err := recordIdentity(body)
			if err != nil {
				return err
			}
			if identity == other {
				if _, err = tx.ExecContext(ctx, "UPDATE events SET body=? WHERE id=?", string(rawEvent), row["id"]); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if change.DiscoveredAt > 0 {
		if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO order_discovery VALUES(?,?,?,?)", change.Key.Portfolio, key, change.DiscoveredAt, change.DiscoveredAt-change.Key.UpdateTime); err != nil {
			return err
		}
	}
	var name string
	err = tx.QueryRowContext(ctx, "SELECT name FROM targets WHERE id=?", change.Key.Portfolio).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "INSERT INTO events(target,name,kind,body,stamp,historical) VALUES(?,?,'order',?,?,0)", change.Key.Portfolio, name, string(rawEvent), stamp)
	if err != nil {
		return err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	return queueEvent(ctx, tx, id)
}
