package store

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
)

func (s *SQLiteStore) SaveLiveHistory(ctx context.Context, target string, fills []LeaderFill) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var active bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM leader_history_sync WHERE target=? AND complete=0 AND started_at>0)", target).Scan(&active); err != nil {
		return err
	}
	for _, fill := range fills {
		fill.Symbol = strings.ToUpper(strings.TrimSpace(fill.Symbol))
		fill.Side = strings.ToUpper(strings.TrimSpace(fill.Side))
		fill.PositionSide = strings.ToUpper(strings.TrimSpace(fill.PositionSide))
		key, err := historyIdentity(fill)
		if err != nil {
			return err
		}
		stamp, err := strconv.ParseInt(fill.Created.String(), 10, 64)
		if err != nil {
			return err
		}
		var body map[string]any
		raw, err := json.Marshal(fill)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if err = decoder.Decode(&body); err != nil {
			return err
		}
		body["symbol"], body["side"], body["positionSide"], body["orderTime"] = fill.Symbol, fill.Side, fill.PositionSide, stamp
		raw, err = json.Marshal(body)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO leader_history VALUES(?,?,?,?)", target, key, stamp, string(raw)); err != nil {
			return err
		}
		if active {
			if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO leader_history_stage VALUES(?,?,?,?)", target, key, stamp, string(raw)); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}
