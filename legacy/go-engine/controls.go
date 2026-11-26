package engine

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"regexp"
	"strings"
	"time"
)

type ControlInput struct {
	ID         string `json:"id"`
	AccountID  *int64 `json:"account_id"`
	TargetID   string `json:"target_id"`
	Enabled    *bool  `json:"enabled"`
	Selected   *bool  `json:"selected"`
	Active     *bool  `json:"active"`
	Multiplier string `json:"multiplier"`
}

type InputError string

func (e InputError) Error() string { return string(e) }

var multiplierPattern = regexp.MustCompile(`^[0-9]{1,3}(\.[0-9]{1,4})?$`)

func (s *SQLiteStore) ApplyControl(ctx context.Context, path string, in ControlInput, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if path == "/api/toggle" {
		if in.Enabled == nil {
			return InputError("enabled 必须是布尔值")
		}
		res, err := tx.ExecContext(ctx, "UPDATE targets SET enabled=?,next_check=0 WHERE id=?", *in.Enabled, in.ID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return InputError("监听对象不存在")
		}
		if !*in.Enabled {
			for _, stmt := range []string{
				"UPDATE outbox SET status='cancelled' WHERE status='pending' AND event IN (SELECT id FROM events WHERE target=?)",
				"UPDATE copy_tasks SET state='cancelled',error='监听已关闭' WHERE target_id=? AND state='pending'",
				"UPDATE copy_live SET enabled=0 WHERE target_id=?", "UPDATE virtual_sessions SET active=0 WHERE target_id=?",
			} {
				if _, err := tx.ExecContext(ctx, stmt, in.ID); err != nil {
					return err
				}
			}
		}
		return tx.Commit()
	}
	if in.AccountID == nil {
		return InputError("账号参数错误")
	}
	id, target := *in.AccountID, in.TargetID
	execPair := func(stmt string) error { _, err := tx.ExecContext(ctx, stmt, id, target); return err }
	stop := func() error {
		for _, stmt := range []string{"UPDATE copy_live SET enabled=0 WHERE account_id=? AND target_id=?", "UPDATE virtual_sessions SET active=0 WHERE account_id=? AND target_id=?", "UPDATE copy_tasks SET state='cancelled',error='跟单已关闭' WHERE account_id=? AND target_id=? AND state='pending'"} {
			if err := execPair(stmt); err != nil {
				return err
			}
		}
		return nil
	}
	if path == "/api/copy-binding" {
		if in.Selected == nil {
			return InputError("关联参数错误")
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM futures_accounts WHERE id=?) AND EXISTS(SELECT 1 FROM targets WHERE id=?)", id, target).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return InputError("账号或带单员不存在")
		}
		if *in.Selected {
			err = execPair("INSERT OR IGNORE INTO copy_bindings VALUES(?,?)")
		} else {
			if err = stop(); err != nil {
				return err
			}
			for _, stmt := range []string{"DELETE FROM copy_multipliers WHERE account_id=? AND target_id=?", "DELETE FROM copy_multiplier_settings_v2 WHERE account_id=? AND target_id=?", "DELETE FROM copy_bindings WHERE account_id=? AND target_id=?"} {
				if err = execPair(stmt); err != nil {
					return err
				}
			}
		}
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	var enabled, initialized bool
	err = tx.QueryRowContext(ctx, "SELECT t.enabled,t.initialized FROM copy_bindings b JOIN targets t ON t.id=b.target_id WHERE b.account_id=? AND b.target_id=?", id, target).Scan(&enabled, &initialized)
	if errors.Is(err, sql.ErrNoRows) {
		return InputError("请先关联带单员")
	}
	if err != nil {
		return err
	}
	switch path {
	case "/api/copy-multiplier":
		value, e := positiveDecimal(in.Multiplier, "multiplier")
		if !multiplierPattern.MatchString(in.Multiplier) || e != nil || value.Cmp(big.NewRat(100, 1)) > 0 {
			return InputError("倍数必须为 0.0001 至 100 的数字，最多四位小数")
		}
		whole, fraction, ok := strings.Cut(in.Multiplier, ".")
		whole = strings.TrimLeft(whole, "0")
		if whole == "" {
			whole = "0"
		}
		if ok {
			whole += "." + fraction
		}
		_, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO copy_multiplier_settings_v2 VALUES(?,?,?)", id, target, whole)
	case "/api/virtual-session":
		if in.Active == nil {
			return InputError("参数错误")
		}
		if !*in.Active {
			err = stop()
			break
		}
		if !enabled || !initialized {
			return InputError("请先开启监听并完成首次同步")
		}
		var exists bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM virtual_sessions WHERE account_id=? AND target_id=?)", id, target).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return InputError("已有跟踪记录，不能重置为零")
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO virtual_sessions VALUES(?,?,?,1)", id, target, now.UnixMilli())
	case "/api/copy-live":
		if in.Enabled == nil {
			return InputError("参数错误")
		}
		if !*in.Enabled {
			err = stop()
			break
		}
		if !enabled {
			return InputError("请先开启监听")
		}
		var live, unknown bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM copy_live WHERE account_id=? AND target_id=? AND enabled=1),EXISTS(SELECT 1 FROM copy_tasks WHERE account_id=? AND state='submitted')", id, target, id).Scan(&live, &unknown); err != nil {
			return err
		}
		if live {
			return tx.Commit()
		}
		if unknown {
			return InputError("该账号有结果未确认的订单，请先核对")
		}
		if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO virtual_sessions VALUES(?,?,?,1)", id, target, now.UnixMilli()); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO copy_live VALUES(?,?,1,?)", id, target, now.UnixMilli()); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "UPDATE targets SET next_check=0 WHERE id=?", target)
	default:
		return InputError("未知操作")
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}
