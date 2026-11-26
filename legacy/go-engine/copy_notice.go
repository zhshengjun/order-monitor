package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"time"
)

func copyNotice(ctx context.Context, tx *sql.Tx, target string, body map[string]any) error {
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
	var raw string
	if err := tx.QueryRowContext(ctx, "SELECT value FROM settings WHERE id=1").Scan(&raw); errors.Is(err, sql.ErrNoRows) {
		return nil
	} else if err != nil {
		return err
	}
	var settings struct {
		Telegram bool `json:"telegram_enabled"`
		Bark     bool `json:"bark_enabled"`
	}
	if err := json.Unmarshal([]byte(raw), &settings); err != nil {
		return err
	}
	for channel, enabled := range map[string]bool{"telegram": settings.Telegram, "bark": settings.Bark} {
		if enabled {
			if _, err := tx.ExecContext(ctx, "INSERT INTO outbox(event,channel) VALUES(?,?)", id, channel); err != nil {
				return err
			}
		}
	}
	return nil
}

func executionNotional(result OrderResult, filled *big.Rat) *big.Rat {
	if filled.Sign() == 0 {
		return new(big.Rat)
	}
	if q, err := positiveDecimal(result.Quote, "cumQuote"); err == nil {
		return q
	}
	if price, err := positiveDecimal(result.Average, "avgPrice"); err == nil {
		return price.Mul(price, filled)
	}
	return nil
}

func noticeNumber(n *big.Rat) string {
	if n == nil {
		return "—"
	}
	value := strings.TrimRight(strings.TrimRight(n.FloatString(4), "0"), ".")
	if value == "-0" {
		return "0"
	}
	return value
}
