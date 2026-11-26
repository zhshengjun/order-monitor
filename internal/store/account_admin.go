package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"unicode/utf8"

	"modernc.org/sqlite"
)

type AccountInput struct {
	ID     *int64 `json:"account_id"`
	Name   string `json:"name"`
	Key    string `json:"api_key"`
	Secret string `json:"api_secret"`
}

func (s *SQLiteStore) SaveAccount(ctx context.Context, in AccountInput) error {
	name, key, secret := strings.TrimSpace(in.Name), strings.TrimSpace(in.Key), strings.TrimSpace(in.Secret)
	if name == "" || utf8.RuneCountInString(name) > 256 || utf8.RuneCountInString(key) > 256 || utf8.RuneCountInString(secret) > 256 || (key == "") != (secret == "") {
		return InputError("请填写账号名称；更新密钥时须同时填写 API Key 和 Secret Key")
	}
	if in.ID == nil && key == "" {
		return InputError("请填写 API Key 和 Secret Key")
	}
	if in.ID != nil && *in.ID <= 0 {
		return InputError("账号参数错误")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if in.ID == nil {
		_, err = tx.ExecContext(ctx, "INSERT INTO futures_accounts(name,api_key,api_secret) VALUES(?,?,?)", name, key, secret)
	} else {
		id := *in.ID
		var exists bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM futures_accounts WHERE id=?)", id).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return InputError("账号不存在")
		}
		if key == "" {
			_, err = tx.ExecContext(ctx, "UPDATE futures_accounts SET name=? WHERE id=?", name, id)
		} else {
			if err = canReplaceCredentials(ctx, tx, id); err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "UPDATE futures_accounts SET name=?,api_key=?,api_secret=? WHERE id=?", name, key, secret, id)
		}
	}
	var constraint *sqlite.Error
	if errors.As(err, &constraint) && constraint.Code() == 2067 {
		return InputError("账号名称已存在")
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func canReplaceCredentials(ctx context.Context, tx *sql.Tx, id int64) error {
	var manual, active bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM manual_closes WHERE account_id=? AND state IN ('submitted','needs_review')),
	EXISTS(SELECT 1 FROM copy_live WHERE account_id=? AND enabled=1 UNION ALL SELECT 1 FROM copy_tasks WHERE account_id=? AND state IN ('pending','submitted'))`, id, id, id).Scan(&manual, &active)
	if err != nil {
		return err
	}
	if manual {
		return InputError("该账户有待确认的手动平仓，无法更换密钥")
	}
	const outstanding = InputError("请先停止跟单并核对未完成订单及跟单持仓，再更换密钥")
	if active {
		return outstanding
	}
	rows, err := tx.QueryContext(ctx, "SELECT quantity FROM copied_positions WHERE account_id=?", id)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		q, err := decimal(raw)
		if err != nil {
			return err
		}
		if q.Sign() != 0 {
			return outstanding
		}
	}
	return rows.Err()
}
