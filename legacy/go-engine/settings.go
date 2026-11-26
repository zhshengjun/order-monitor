package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"unicode/utf8"
)

func defaultSettings() map[string]any {
	return map[string]any{"interval": 60, "page_size": 20, "balance_threshold": "0", "telegram_enabled": false, "telegram_token": "", "telegram_chat": "", "bark_enabled": false, "bark_url": "https://api.day.app", "bark_key": ""}
}

func (s *SQLiteStore) SaveSettings(ctx context.Context, patch map[string]any) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	settings := defaultSettings()
	var old string
	err = tx.QueryRowContext(ctx, "SELECT value FROM settings WHERE id=1").Scan(&old)
	if err == nil {
		if err = json.Unmarshal([]byte(old), &settings); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	for key := range defaultSettings() {
		value, ok := patch[key]
		if !ok {
			continue
		}
		if (key == "telegram_token" || key == "bark_key") && value == "" {
			continue
		}
		settings[key] = value
	}
	for _, key := range []string{"telegram_token", "bark_key"} {
		if patch["clear_"+key] == true {
			settings[key] = ""
		}
	}
	for _, field := range []struct {
		name     string
		min, max int
		message  string
	}{{"interval", 10, 86400, "监听频率必须为 10～86400 秒"}, {"page_size", 1, 20, "轮询每页数量必须为 1～20 条"}} {
		n, err := strconv.Atoi(fmt.Sprint(settings[field.name]))
		if err != nil || n < field.min || n > field.max {
			return InputError(field.message)
		}
		settings[field.name] = n
	}
	threshold, err := decimal(fmt.Sprint(settings["balance_threshold"]))
	if err != nil {
		return InputError("余额通知阈值必须为非负数字")
	}
	settings["balance_threshold"] = decimalString(threshold)
	for _, key := range []string{"telegram_enabled", "bark_enabled"} {
		if _, ok := settings[key].(bool); !ok {
			return InputError("通知开关必须是布尔值")
		}
	}
	for _, key := range []string{"telegram_token", "telegram_chat", "bark_key", "bark_url"} {
		text, ok := settings[key].(string)
		if !ok || utf8.RuneCountInString(text) > 1000 {
			return InputError("通知配置格式错误")
		}
	}
	u, err := url.Parse(settings["bark_url"].(string))
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return InputError("Bark 地址必须为不含账号、参数的 HTTPS 地址")
	}
	if settings["telegram_enabled"] == true && (settings["telegram_token"] == "" || settings["telegram_chat"] == "") {
		return InputError("启用 Telegram 需要 Token 和 Chat ID")
	}
	if settings["bark_enabled"] == true && settings["bark_key"] == "" {
		return InputError("启用 Bark 需要 Device Key")
	}
	raw, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO settings(id,value) VALUES(1,?)", string(raw)); err != nil {
		return err
	}
	return tx.Commit()
}
