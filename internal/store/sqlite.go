package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	_ "modernc.org/sqlite"
)

// SQLiteStore durably records fill baselines and new fill work in the same
// transaction. It uses its own tables and can be reopened by a new process.
type SQLiteStore struct {
	db *sql.DB
}

func OpenSQLiteStore(ctx context.Context, path string) (*SQLiteStore, error) {
	if path == "" || path == ":memory:" {
		return nil, errors.New("persistent SQLite path required")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.ExecContext(ctx, `
		PRAGMA journal_mode=WAL;
		PRAGMA synchronous=FULL;
		CREATE TABLE IF NOT EXISTS go_legacy_order_keys (order_key TEXT PRIMARY KEY, original_key TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS go_fill_baselines (
			order_key TEXT PRIMARY KEY,
			quantity TEXT NOT NULL,
			cost TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS go_fill_work (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			body TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS go_submissions (
			task_key TEXT PRIMARY KEY,
			client_id TEXT NOT NULL UNIQUE,
			order_body TEXT NOT NULL,
			state TEXT NOT NULL,
			result TEXT
		);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize fill store: %w", err)
	}
	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Close() error { return s.db.Close() }

// DB exposes the shared connection to feature packages in this module.
func (s *SQLiteStore) DB() *sql.DB { return s.db }

func (s *SQLiteStore) Load(ctx context.Context, key OrderKey) (CumulativeFill, error) {
	encoded, err := json.Marshal(key)
	if err != nil {
		return CumulativeFill{}, err
	}
	var fill CumulativeFill
	err = s.db.QueryRowContext(ctx, "SELECT quantity,cost FROM go_fill_baselines WHERE order_key=?", string(encoded)).
		Scan(&fill.Quantity, &fill.Cost)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return fill, err
}

func (s *SQLiteStore) Commit(ctx context.Context, change Comparison) error {
	key, err := json.Marshal(change.Key)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var result sql.Result
	if change.Previous == (CumulativeFill{}) {
		result, err = tx.ExecContext(ctx,
			"INSERT INTO go_fill_baselines VALUES(?,?,?) ON CONFLICT(order_key) DO NOTHING",
			string(key), change.Fill.Quantity, change.Fill.Cost)
	} else {
		result, err = tx.ExecContext(ctx,
			"UPDATE go_fill_baselines SET quantity=?,cost=? WHERE order_key=? AND quantity=? AND cost=?",
			change.Fill.Quantity, change.Fill.Cost, string(key), change.Previous.Quantity, change.Previous.Cost)
	}
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return errors.New("fill baseline changed before commit")
	}

	if change.Record != nil {
		if err := saveOrderRecord(ctx, tx, change); err != nil {
			return err
		}
	}
	if change.QuantityDelta != "" {
		body, err := json.Marshal(change)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO go_fill_work(body) VALUES(?)", string(body)); err != nil {
			return err
		}
	}
	return tx.Commit()
}
