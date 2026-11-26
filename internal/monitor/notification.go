package monitor

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func sendNotification(ctx context.Context, client *http.Client, settings map[string]any, channel, title, message string) error {
	var endpoint string
	var body map[string]any
	switch channel {
	case "telegram":
		endpoint = "https://api.telegram.org/bot" + settings["telegram_token"].(string) + "/sendMessage"
		body = map[string]any{"chat_id": settings["telegram_chat"], "text": title + "\n" + message}
	case "bark":
		endpoint = strings.TrimRight(settings["bark_url"].(string), "/") + "/push"
		body = map[string]any{"device_key": settings["bark_key"], "title": title, "body": message, "group": "订单监听"}
	default:
		return errors.New("未知通知渠道")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return errors.New("通知配置格式错误")
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		return errors.New("通知服务网络连接失败")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("通知服务 HTTP %d", response.StatusCode)
	}
	var payload struct {
		OK        bool `json:"ok"`
		Code      int  `json:"code"`
		ErrorCode int  `json:"error_code"`
	}
	if err = json.NewDecoder(io.LimitReader(response.Body, 1000000)).Decode(&payload); err != nil {
		return errors.New("通知服务响应不是有效 JSON")
	}
	if channel == "telegram" && !payload.OK || channel == "bark" && payload.Code != 200 {
		return errors.New("通知服务拒绝请求")
	}
	return nil
}
func notificationText(name, kind string, stamp int64, body map[string]any) (string, string) {
	labels := map[string]string{"copy": "跟单结果", "margin": "保证金风险", "order": "交易操作", "transfer": "资金划转", "alert": "告警"}
	title := name + " · " + labels[kind]
	value := func(key string) string {
		v := body[key]
		if v == nil {
			return ""
		}
		return fmt.Sprint(v)
	}
	text := value("message")
	if kind == "alert" {
		text = fmt.Sprintf("【%s】\n%s", value("title"), value("message"))
	}
	if kind == "transfer" {
		text = fmt.Sprintf("%s\n%s %s\n%s → %s", value("transType"), value("amount"), value("coin"), value("from"), value("to"))
	}
	if kind == "order" {
		direction := map[[2]string]string{{"BUY", "LONG"}: "开多", {"SELL", "LONG"}: "平多", {"SELL", "SHORT"}: "开空", {"BUY", "SHORT"}: "平空"}[[2]string{value("side"), value("positionSide")}]
		text = fmt.Sprintf("%s %s\n数量：%s %s\n均价：%s %s\n成交金额：%s %s\n已实现盈亏：%s %s", value("symbol"), direction, value("executedQty"), value("baseAsset"), value("avgPrice"), value("quoteAsset"), value("notional"), value("quoteAsset"), value("totalPnl"), value("quoteAsset"))
	}
	return title, text + "\n时间（UTC+8）：" + time.UnixMilli(stamp).In(time.FixedZone("UTC+8", 8*3600)).Format("2006-01-02 15:04:05")
}
func (s *Service) deliverNext(ctx context.Context, client *http.Client) (bool, error) {
	var id, event, stamp int64
	var attempts int
	var channel, kind, name, bodyRaw, target string
	err := s.DB().QueryRowContext(ctx, `SELECT o.id,o.event,o.channel,o.attempts,e.kind,e.name,e.body,e.stamp,e.target FROM outbox o JOIN events e ON e.id=o.event WHERE o.status='pending' AND e.kind!='balance' AND o.next_try<=? ORDER BY o.id LIMIT 1`, float64(time.Now().UnixMilli())/1000).Scan(&id, &event, &channel, &attempts, &kind, &name, &bodyRaw, &stamp, &target)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var settingsRaw string
	err = s.DB().QueryRowContext(ctx, "SELECT value FROM settings WHERE id=1").Scan(&settingsRaw)
	if errors.Is(err, sql.ErrNoRows) {
		settingsRaw = "{}"
	} else if err != nil {
		return false, err
	}
	settings := defaultSettings()
	if err = json.Unmarshal([]byte(settingsRaw), &settings); err != nil {
		return false, err
	}
	var body map[string]any
	if err = json.Unmarshal([]byte(bodyRaw), &body); err != nil {
		return false, err
	}
	if settings[channel+"_enabled"] != true {
		_, err = s.DB().ExecContext(ctx, "UPDATE outbox SET status='cancelled' WHERE id=? AND status='pending'", id)
		return true, err
	}
	if kind == "order" {
		var f LeaderFill
		if err = json.Unmarshal([]byte(bodyRaw), &f); err != nil {
			return false, err
		}
		var update int64
		if f.Updated != "" {
			update, err = f.Updated.Int64()
		}
		if err != nil || update <= 0 || time.Now().UnixMilli()-update > 180000 {
			_, err = s.DB().ExecContext(ctx, "UPDATE outbox SET status='cancelled',error='订单通知已过期' WHERE id=? AND status='pending'", id)
			return true, err
		}
	}
	if kind == "copy" {
		if err = s.enrichCopy(ctx, client, event, body); err != nil {
			return false, err
		}
	}
	mergedID, leaderTitle, leaderMessage, wait, err := s.notificationMerge(ctx, target, channel, kind, body)
	if err != nil {
		return false, err
	}
	if wait {
		_, err = s.DB().ExecContext(ctx, "UPDATE outbox SET next_try=? WHERE id=? AND status='pending'", float64(time.Now().UnixMilli())/1000+1, id)
		return true, err
	}
	title, message := notificationText(name, kind, stamp, body)
	if mergedID != 0 {
		lines := strings.Split(leaderMessage, "\n")
		copyLines := strings.Split(message, "\n")
		header := lines[0]
		lines[len(lines)-1] = strings.Replace(lines[len(lines)-1], "时间（UTC+8）：", "订单时间（UTC+8）：", 1)
		copyLines[len(copyLines)-1] = strings.Replace(copyLines[len(copyLines)-1], "时间（UTC+8）：", "结果时间（UTC+8）：", 1)
		prefix := header + "，状态 "
		if strings.HasPrefix(copyLines[0], prefix) {
			status := strings.TrimPrefix(copyLines[0], prefix)
			if status == "FILLED" {
				status = "已成交（FILLED）"
			}
			copyLines[0] = "状态：" + status
		}
		title = strings.Replace(leaderTitle, "交易操作", "交易与跟单", 1)
		message = header + "\n\n【带单交易】\n" + strings.Join(lines[1:], "\n") + "\n\n【我的跟单】\n" + strings.Join(copyLines, "\n")
	}
	err = sendNotification(ctx, client, settings, channel, title, message)
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if err == nil {
		tx, e := s.DB().BeginTx(ctx, nil)
		if e != nil {
			return false, e
		}
		defer tx.Rollback()
		if _, e = tx.ExecContext(ctx, "UPDATE outbox SET status='sent',error='' WHERE id=?", id); e != nil {
			return false, e
		}
		if mergedID != 0 {
			if _, e = tx.ExecContext(ctx, "UPDATE outbox SET status='sent',error='' WHERE id=? AND status='pending'", mergedID); e != nil {
				return false, e
			}
		}
		return true, tx.Commit()
	}
	attempts++
	delay := min(3600, 10*(1<<min(attempts, 8)))
	_, saveErr := s.DB().ExecContext(ctx, "UPDATE outbox SET attempts=?,next_try=?,error=? WHERE id=?", attempts, time.Now().Unix()+int64(delay), err.Error(), id)
	return true, errors.Join(err, saveErr)
}
func (s *Service) RunNotifications(ctx context.Context, client *http.Client) {
	for ctx.Err() == nil {
		worked, err := s.deliverNext(ctx, client)
		if err == nil && worked {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func validBarkURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

func (s *Service) TestNotification(ctx context.Context, client *http.Client, patch map[string]any) error {
	channel, ok := patch["channel"].(string)
	if !ok || (channel != "telegram" && channel != "bark") {
		return InputError("未知通知渠道")
	}
	settings := defaultSettings()
	var raw string
	err := s.DB().QueryRowContext(ctx, "SELECT value FROM settings WHERE id=1").Scan(&raw)
	if err == nil {
		if err = json.Unmarshal([]byte(raw), &settings); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	keys := []string{"telegram_token", "telegram_chat"}
	if channel == "bark" {
		keys = []string{"bark_url", "bark_key"}
	}
	for _, key := range keys {
		value, exists := patch[key]
		if !exists {
			continue
		}
		text, ok := value.(string)
		if !ok || len([]rune(text)) > 1000 {
			return InputError("通知配置格式错误")
		}
		if text != "" {
			settings[key] = text
		}
	}
	if channel == "telegram" && (settings["telegram_token"] == "" || settings["telegram_chat"] == "") {
		return InputError("请填写 Telegram Token 和 Chat ID")
	}
	if channel == "bark" {
		if !validBarkURL(settings["bark_url"].(string)) {
			return InputError("Bark 地址必须为不含账号、参数的 HTTPS 地址")
		}
		if settings["bark_key"] == "" {
			return InputError("请填写 Bark Device Key")
		}
	}
	if err = sendNotification(ctx, client, settings, channel, "订单监听", "通知测试成功"); err != nil {
		return InputError("测试发送失败：" + err.Error())
	}
	return nil
}
