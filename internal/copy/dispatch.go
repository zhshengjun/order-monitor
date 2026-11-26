package copy

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"order-monitor/internal/store"
	"strconv"
	"time"
)

// DispatchNext applies a queued fill to the existing per-account virtual ledger.
// It commits the account tasks, their multiplier snapshots, and dequeue together.
func (s *Service) DispatchNext(ctx context.Context, now time.Time) (bool, error) {
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var id int64
	var body string
	err = tx.QueryRowContext(ctx, "SELECT id,body FROM go_fill_work ORDER BY id LIMIT 1").Scan(&id, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var change Comparison
	if err = json.Unmarshal([]byte(body), &change); err != nil {
		// A corrupt work row must never block every later fill.
		if _, err = tx.ExecContext(ctx, "DELETE FROM go_fill_work WHERE id=?", id); err != nil {
			return false, err
		}
		return true, tx.Commit()
	}
	fresh := change.Key.UpdateTime <= now.Add(5*time.Second).UnixMilli() && change.Key.UpdateTime >= now.Add(-3*time.Minute).UnixMilli()
	if fresh {
		err = dispatchFill(ctx, tx, change, now)
	} else {
		err = skipStaleFill(ctx, tx, change, now)
	}
	if err != nil {
		if !errors.Is(err, errInvalidFill) {
			return false, err
		}
		// Permanent data errors are dead-lettered with an operator notice;
		// retrying the same row would block all subsequent fills forever.
		if noticeErr := copyNotice(ctx, tx, change.Key.Portfolio, map[string]any{"message": fmt.Sprintf("%s 跟单数据异常，已跳过：%v", change.Key.Symbol, err)}); noticeErr != nil {
			return false, noticeErr
		}
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM go_fill_work WHERE id=?", id); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

var errInvalidFill = errors.New("invalid fill work")

func invalidFill(err error) error { return fmt.Errorf("%w: %v", errInvalidFill, err) }

type accountBinding struct {
	id         int64
	multiplier string
}

type sessionState struct {
	id                      int64
	live, active            bool
	liveStart, sessionStart int64
}

// dispatchFill mirrors the Python virtual ledger: every active session records
// the leader fill, while only sessions with copy enabled enqueue an order.
func dispatchFill(ctx context.Context, tx *sql.Tx, change Comparison, now time.Time) error {
	k := change.Key
	base, err := orderBaseKey(ctx, tx, k)
	if err != nil {
		return err
	}
	increment := change.Previous != (CumulativeFill{})
	key := base
	if increment {
		qty, err := positiveDecimal(change.Fill.Quantity, "cumulative quantity")
		if err != nil {
			return invalidFill(err)
		}
		key += ":" + decimalString(qty)
	}
	if err = ensureSessions(ctx, tx, k.Portfolio, k.Symbol); err != nil {
		return err
	}
	qty, err := positiveDecimal(change.QuantityDelta, "increment quantity")
	if err != nil {
		return invalidFill(err)
	}
	cost, err := positiveDecimal(change.CostDelta, "increment cost")
	if err != nil {
		return invalidFill(err)
	}
	price := decimalText(new(big.Rat).Quo(cost, qty))

	followed := false
	if increment {
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM virtual_events WHERE target_id=? AND order_key=?)", k.Portfolio, base).Scan(&followed); err != nil {
			return err
		}
	}
	standalone := increment && !followed

	settings, err := multiplierSettings(ctx, tx, k.Portfolio)
	if err != nil {
		return err
	}

	sessions, err := activeSessions(ctx, tx, k.Portfolio)
	if err != nil {
		return err
	}
	for _, session := range sessions {
		if k.UpdateTime <= session.started {
			continue
		}
		var exists bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM virtual_events WHERE account_id=? AND target_id=? AND order_key=?)", session.account, k.Portfolio, key).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		var latest sql.NullInt64
		if err = tx.QueryRowContext(ctx, "SELECT MAX(stamp) FROM virtual_events WHERE account_id=? AND target_id=? AND symbol=? AND position_side=?", session.account, k.Portfolio, k.Symbol, k.PositionSide).Scan(&latest); err != nil {
			return err
		}
		if latest.Valid && latest.Int64 > k.UpdateTime {
			for _, stmt := range []string{"UPDATE copy_live SET enabled=0 WHERE account_id=? AND target_id=?", "UPDATE virtual_sessions SET active=0 WHERE account_id=? AND target_id=?", "UPDATE copy_tasks SET state='cancelled',error='交易记录乱序' WHERE account_id=? AND target_id=? AND state='pending'"} {
				if _, err := tx.ExecContext(ctx, stmt, session.account, k.Portfolio); err != nil {
					return err
				}
			}
			if err = store.SetVirtualTrust(ctx, tx, session.account, k.Portfolio, false, "交易记录延迟到达"); err != nil {
				return err
			}
			if err = copyNotice(ctx, tx, k.Portfolio, map[string]any{"message": k.Symbol + " 交易记录延迟到达，顺序无法确认，已暂停跟单，请核对持仓"}); err != nil {
				return err
			}
			continue
		}
		var before string
		err = tx.QueryRowContext(ctx, "SELECT quantity FROM virtual_positions WHERE account_id=? AND target_id=? AND symbol=? AND position_side=?", session.account, k.Portfolio, k.Symbol, k.PositionSide).Scan(&before)
		if errors.Is(err, sql.ErrNoRows) {
			before = "0"
		} else if err != nil {
			return err
		}
		position, err := TransitionLeaderPosition(before, k.Side, k.PositionSide, change.QuantityDelta)
		if err != nil {
			return invalidFill(err)
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO virtual_events VALUES(?,?,?,?,?,?,?,?,?,?)", session.account, k.Portfolio, key, k.Symbol, k.PositionSide, k.UpdateTime, position.Action, change.QuantityDelta, before, position.After); err != nil {
			return err
		}

		var liveEnabled int
		var liveStart int64
		err = tx.QueryRowContext(ctx, "SELECT enabled,started_at FROM copy_live WHERE account_id=? AND target_id=?", session.account, k.Portfolio).Scan(&liveEnabled, &liveStart)
		if errors.Is(err, sql.ErrNoRows) {
			liveEnabled, liveStart, err = 0, 0, nil
		}
		if err != nil {
			return err
		}
		if liveEnabled == 0 || k.UpdateTime <= liveStart {
			if position.Action != "unmatched_close" {
				if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO virtual_positions VALUES(?,?,?,?,?)", session.account, k.Portfolio, k.Symbol, k.PositionSide, position.After); err != nil {
					return err
				}
			}
			continue
		}

		body, err := taskSignal(change, increment, standalone, price)
		if err != nil {
			return invalidFill(err)
		}
		hash := sha256.Sum256([]byte(strconv.FormatInt(session.account, 10) + ":" + k.Portfolio + ":" + key))
		clientID := "cp_" + hex.EncodeToString(hash[:15])
		res, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,created) VALUES(?,?,?,?,?,?,?,?,?)", session.account, k.Portfolio, key, string(body), position.Action, before, change.LeaderBalance, clientID, now.UnixMilli())
		if err != nil {
			return fmt.Errorf("enqueue copy task: %w", err)
		}
		inserted, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if inserted == 1 {
			task, err := res.LastInsertId()
			if err != nil {
				return err
			}
			multiplier := settings[session.account]
			if multiplier == "" {
				multiplier = "1"
			}
			if increment && !standalone {
				var prior string
				err = tx.QueryRowContext(ctx, `SELECT COALESCE(m.multiplier,'1') FROM copy_tasks c LEFT JOIN copy_task_multiplier_v2 m ON m.task_id=c.id WHERE c.account_id=? AND c.target_id=? AND (c.order_key=? OR c.order_key LIKE ?) AND c.id<? ORDER BY c.id LIMIT 1`, session.account, k.Portfolio, base, base+":%", task).Scan(&prior)
				switch {
				case err == nil:
					multiplier = prior
				case errors.Is(err, sql.ErrNoRows):
				default:
					return err
				}
			}
			if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO copy_task_multiplier_v2 VALUES(?,?)", task, multiplier); err != nil {
				return err
			}
			executed, err := positiveDecimal(change.QuantityDelta, "leader reduction")
			if err != nil {
				return invalidFill(err)
			}
			leaderBefore, err := decimal(before)
			if err != nil {
				return err
			}
			if (position.Action == "close" || position.Action == "unmatched_close") && leaderBefore.Sign() > 0 && executed.Cmp(leaderBefore) >= 0 {
				if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO close_watch(account_id,target_id,symbol,position_side,order_key,task_id) VALUES(?,?,?,?,?,?)", session.account, k.Portfolio, k.Symbol, k.PositionSide, key, task); err != nil {
					return err
				}
			}
		}
		if position.Action != "unmatched_close" {
			if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO virtual_positions VALUES(?,?,?,?,?)", session.account, k.Portfolio, k.Symbol, k.PositionSide, position.After); err != nil {
				return err
			}
		}
	}
	return nil
}

// orderBaseKey returns the legacy-compatible identity of the order group.
func orderBaseKey(ctx context.Context, tx *sql.Tx, k OrderKey) (string, error) {
	raw, err := json.Marshal([]any{k.Symbol, k.Side, k.PositionSide, k.UpdateTime})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	base := hex.EncodeToString(digest[:])
	encoded, err := json.Marshal(k)
	if err != nil {
		return "", err
	}
	var legacy string
	err = tx.QueryRowContext(ctx, "SELECT original_key FROM go_legacy_order_keys WHERE order_key=?", string(encoded)).Scan(&legacy)
	if err == nil {
		return legacy, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	return base, nil
}

// ensureSessions mirrors the Python repair step: an enabled copy binding whose
// virtual session is missing or inactive is restarted from the known start time.
func ensureSessions(ctx context.Context, tx *sql.Tx, target, symbol string) error {
	rows, err := tx.QueryContext(ctx, `SELECT b.account_id,COALESCE(l.enabled,0),COALESCE(l.started_at,0),COALESCE(v.active,0),COALESCE(v.started_at,0)
	FROM copy_bindings b LEFT JOIN copy_live l ON l.account_id=b.account_id AND l.target_id=b.target_id
	LEFT JOIN virtual_sessions v ON v.account_id=b.account_id AND v.target_id=b.target_id
	WHERE b.target_id=? ORDER BY b.account_id`, target)
	if err != nil {
		return err
	}
	var states []sessionState
	for rows.Next() {
		var state sessionState
		if err = rows.Scan(&state.id, &state.live, &state.liveStart, &state.active, &state.sessionStart); err != nil {
			rows.Close()
			return err
		}
		states = append(states, state)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, state := range states {
		if !state.live || state.active {
			continue
		}
		start := max(state.liveStart, state.sessionStart)
		if _, err = tx.ExecContext(ctx, `INSERT INTO virtual_sessions(account_id,target_id,started_at,active) VALUES(?,?,?,1)
		ON CONFLICT(account_id,target_id) DO UPDATE SET active=1,started_at=excluded.started_at`, state.id, target, start); err != nil {
			return err
		}
		if err = copyNotice(ctx, tx, target, map[string]any{"message": fmt.Sprintf("账户 %d 的跟单状态不一致，已修复并检查 %s 订单", state.id, symbol)}); err != nil {
			return err
		}
	}
	return nil
}

type sessionStart struct {
	account int64
	started int64
}

func multiplierSettings(ctx context.Context, tx *sql.Tx, target string) (map[int64]string, error) {
	rows, err := tx.QueryContext(ctx, "SELECT account_id,multiplier FROM copy_multiplier_settings_v2 WHERE target_id=?", target)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	settings := map[int64]string{}
	for rows.Next() {
		var account int64
		var multiplier string
		if err = rows.Scan(&account, &multiplier); err != nil {
			return nil, err
		}
		settings[account] = multiplier
	}
	return settings, rows.Err()
}

func activeSessions(ctx context.Context, tx *sql.Tx, target string) ([]sessionStart, error) {
	rows, err := tx.QueryContext(ctx, "SELECT account_id,started_at FROM virtual_sessions WHERE target_id=? AND active=1 ORDER BY account_id", target)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sessions []sessionStart
	for rows.Next() {
		var session sessionStart
		if err = rows.Scan(&session.account, &session.started); err != nil {
			return nil, err
		}
		sessions = append(sessions, session)
	}
	return sessions, rows.Err()
}

// taskSignal keeps the original leader row fields (symbol, assets, orderTime)
// and replaces the cumulative values with the observed increment, matching the
// Python task body.
func taskSignal(change Comparison, increment, standalone bool, price string) ([]byte, error) {
	signal := make(map[string]any, len(change.Record)+8)
	for field, value := range change.Record {
		signal[field] = value
	}
	signal["symbol"] = change.Key.Symbol
	signal["side"] = change.Key.Side
	signal["positionSide"] = change.Key.PositionSide
	if _, ok := signal["orderTime"]; !ok || signal["orderTime"] == nil {
		signal["orderTime"] = change.Key.UpdateTime
	}
	signal["orderUpdateTime"] = change.Key.UpdateTime
	signal["executedQty"] = change.QuantityDelta
	signal["avgPrice"] = price
	signal["notional"] = change.CostDelta
	if increment {
		signal["incremental"] = true
	}
	if standalone {
		signal["incremental_standalone"] = true
	}
	return json.Marshal(signal)
}

// skipStaleFill records the alert Python emits when an order is older than the
// three-minute window, without moving the virtual position or placing an order.
func skipStaleFill(ctx context.Context, tx *sql.Tx, change Comparison, now time.Time) error {
	k := change.Key
	var live bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM copy_live WHERE target_id=? AND enabled=1 AND started_at<?)", k.Portfolio, k.UpdateTime).Scan(&live); err != nil {
		return err
	}
	if !live {
		return nil
	}
	accounts, err := tx.QueryContext(ctx, "SELECT account_id FROM copy_live WHERE target_id=? AND enabled=1", k.Portfolio)
	if err != nil {
		return err
	}
	var liveAccounts []int64
	for accounts.Next() {
		var account int64
		if err = accounts.Scan(&account); err != nil {
			accounts.Close()
			return err
		}
		liveAccounts = append(liveAccounts, account)
	}
	if err = accounts.Err(); err != nil {
		accounts.Close()
		return err
	}
	accounts.Close()
	for _, account := range liveAccounts {
		if err = store.SetVirtualTrust(ctx, tx, account, k.Portfolio, false, "存在过期成交未跟单"); err != nil {
			return err
		}
	}
	message := fmt.Sprintf("%s %s 跟单增量跳过：成交更新时间无效或超过三分钟，请核对仓位", k.Symbol, direction(map[string]any{"side": k.Side, "positionSide": k.PositionSide}))
	if change.Previous == (CumulativeFill{}) {
		message = fmt.Sprintf("%s %s 跟单跳过：订单更新时间缺失、无效或超过三分钟，请核对仓位", k.Symbol, direction(map[string]any{"side": k.Side, "positionSide": k.PositionSide}))
	}
	if err := copyNotice(ctx, tx, k.Portfolio, map[string]any{"message": message}); err != nil {
		return err
	}
	closed := (k.Side == "SELL" && k.PositionSide == "LONG") || (k.Side == "BUY" && k.PositionSide == "SHORT")
	if change.Previous != (CumulativeFill{}) || !closed {
		return nil
	}
	base, err := orderBaseKey(ctx, tx, k)
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT v.account_id,v.quantity FROM virtual_positions v
	JOIN copy_live l ON l.account_id=v.account_id AND l.target_id=v.target_id AND l.enabled=1
	WHERE v.target_id=? AND v.symbol=? AND v.position_side=?`, k.Portfolio, k.Symbol, k.PositionSide)
	if err != nil {
		return err
	}
	type position struct {
		account  int64
		quantity string
	}
	var positions []position
	for rows.Next() {
		var p position
		if err = rows.Scan(&p.account, &p.quantity); err != nil {
			rows.Close()
			return err
		}
		positions = append(positions, p)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	executed, err := positiveDecimal(change.QuantityDelta, "leader execution quantity")
	if err != nil {
		return err
	}
	for _, p := range positions {
		qty, err := decimal(p.quantity)
		if err != nil {
			return err
		}
		if qty.Sign() > 0 && executed.Cmp(qty) >= 0 {
			if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO close_watch(account_id,target_id,symbol,position_side,order_key) VALUES(?,?,?,?,?)", p.account, k.Portfolio, k.Symbol, k.PositionSide, base); err != nil {
				return err
			}
		}
	}
	return nil
}
