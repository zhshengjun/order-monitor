package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ImportFillBaselines seeds comparisons without emitting work for executions
// previously observed by the monitor. The marker commits with the entire import.
func (s *SQLiteStore) ImportFillBaselines(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	const migration = "go_fill_baselines_v1"
	var imported bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM app_migrations WHERE name=?)", migration).Scan(&imported); err != nil {
		return err
	}
	if imported {
		return tx.Commit()
	}
	rows, err := tx.QueryContext(ctx, "SELECT target,key,body FROM records WHERE kind='order' ORDER BY target,key")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var target, key, raw string
		if err = rows.Scan(&target, &key, &raw); err != nil {
			return err
		}
		var record struct {
			LeaderFill
			Notional json.Number `json:"notional"`
		}
		if err = json.Unmarshal([]byte(raw), &record); err != nil {
			return fmt.Errorf("restore fill %s/%s: %w", target, key, err)
		}
		if record.Updated == "" {
			continue
		}
		observation, err := record.Observation(target)
		if err != nil {
			return fmt.Errorf("restore fill %s/%s: %w", target, key, err)
		}
		observation.Key.Symbol = strings.ToUpper(strings.TrimSpace(observation.Key.Symbol))
		observation.Key.Side = strings.ToUpper(strings.TrimSpace(observation.Key.Side))
		observation.Key.PositionSide = strings.ToUpper(strings.TrimSpace(observation.Key.PositionSide))
		if record.Notional != "" {
			cost, err := positiveDecimal(record.Notional.String(), "stored cumulative notional")
			if err != nil {
				return err
			}
			observation.Fill.Cost = decimalString(cost)
		}
		encoded, err := json.Marshal(observation.Key)
		if err != nil {
			return err
		}
		if err = seedLegacyFill(ctx, tx, string(encoded), key, observation.Fill); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO app_migrations(name) VALUES(?)", migration); err != nil {
		return err
	}
	return tx.Commit()
}

func seedLegacyFill(ctx context.Context, tx *sql.Tx, encoded, original string, fill CumulativeFill) error {
	if _, err := tx.ExecContext(ctx, "INSERT INTO go_legacy_order_keys(order_key,original_key) VALUES(?,?)", encoded, original); err != nil {
		return fmt.Errorf("legacy order identity conflict: %w", err)
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO go_fill_baselines(order_key,quantity,cost) VALUES(?,?,?) ON CONFLICT(order_key) DO NOTHING", encoded, fill.Quantity, fill.Cost)
	return err
}

// MigrateLegacyHistory applies the last Python history migration atomically.
// Existing backup tables remain available for recovery.
func (s *SQLiteStore) MigrateLegacyHistory(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO settings VALUES(1,?)", `{"interval":60,"page_size":20,"balance_threshold":"0","telegram_enabled":false,"telegram_token":"","telegram_chat":"","bark_enabled":false,"bark_url":"https://api.day.app","bark_key":""}`); err != nil {
		return err
	}
	var complete bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM app_migrations WHERE name='history_identity_v6')").Scan(&complete); err != nil {
		return err
	}
	if !complete {
		if _, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS leader_history_before_v6 AS SELECT * FROM leader_history"); err != nil {
			return err
		}
		rows, err := dashboardRows(ctx, tx, "SELECT target,body FROM leader_history UNION ALL SELECT target,body FROM records WHERE kind='order'")
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM leader_history"); err != nil {
			return err
		}
		for _, row := range rows {
			raw := row["body"].(string)
			var fill LeaderFill
			if err = json.Unmarshal([]byte(raw), &fill); err != nil {
				return err
			}
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
			if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO leader_history VALUES(?,?,?,?)", row["target"], key, stamp, raw); err != nil {
				return err
			}
		}
		for _, query := range []string{"DELETE FROM leader_history_stage", "UPDATE leader_history_sync SET page=1,complete=0,next_check=0,last_batch='',error='',started_at=0,failures=0", "INSERT INTO app_migrations VALUES('history_identity_v6')"} {
			if _, err = tx.ExecContext(ctx, query); err != nil {
				return err
			}
		}
	}
	var pageMigrated bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM app_migrations WHERE name='poll_page_size_20_v2')").Scan(&pageMigrated); err != nil {
		return err
	}
	if !pageMigrated {
		if _, err = tx.ExecContext(ctx, "UPDATE settings SET value=json_set(value,'$.page_size',20) WHERE id=1"); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO app_migrations VALUES('poll_page_size_20_v2')"); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO monitor_start SELECT id,? FROM targets", time.Now().UnixMilli()); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO baseline_watermarks SELECT r.target,r.kind,MAX(r.stamp) FROM records r JOIN targets t ON t.id=r.target WHERE t.initialized=1 GROUP BY r.target,r.kind"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO baseline_watermarks SELECT id,'transfer',0 FROM targets WHERE initialized=1"); err != nil {
		return err
	}
	return tx.Commit()
}
