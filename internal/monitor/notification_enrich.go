package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"order-monitor/internal/exchange"
	"strings"
	"time"
)

func (s *Service) enrichCopy(ctx context.Context, httpClient *http.Client, event int64, body map[string]any) error {
	if body["margin_lookup"] != true {
		return nil
	}
	_, marginChecked := body["margin_checked"]
	_, pnlChecked := body["pnl_checked"]
	action, _ := body["margin_action"].(string)
	if marginChecked && (pnlChecked || action != "close" && action != "unmatched_close") {
		return nil
	}
	var key, secret, sourceRaw, clientID string
	var resultRaw sql.NullString
	err := s.DB().QueryRowContext(ctx, `SELECT a.api_key,a.api_secret,c.body,c.result,c.client_id FROM copy_tasks c JOIN futures_accounts a ON a.id=c.account_id WHERE c.id=?`, body["copy_task_id"]).Scan(&key, &secret, &sourceRaw, &resultRaw, &clientID)
	var source struct {
		Symbol     string
		QuoteAsset string
	}
	var result OrderResult
	if err == nil {
		err = json.Unmarshal([]byte(sourceRaw), &source)
	}
	if err == nil && resultRaw.Valid {
		err = json.Unmarshal([]byte(resultRaw.String), &result)
	}
	var client *FuturesClient
	if err == nil {
		client, err = exchange.NewFuturesClient(httpClient, "https://fapi.binance.com", key, secret)
	}
	qty, _ := decimal(result.Executed)
	if !marginChecked {
		var margin *big.Rat
		if err == nil && qty != nil {
			notional := exchange.ExecutionNotional(result, qty)
			if notional == nil {
				queried, e := client.Order(ctx, source.Symbol, clientID)
				if e == nil && queried.OrderID == result.OrderID && queried.Executed == result.Executed {
					notional = exchange.ExecutionNotional(queried, qty)
				}
			}
			if notional != nil && qty.Sign() > 0 {
				message, _ := body["message"].(string)
				body["message"] = strings.Replace(message, "实际成交均价：— "+source.QuoteAsset, "实际成交均价："+noticeNumber(new(big.Rat).Quo(notional, qty))+" "+source.QuoteAsset, 1)
			}
			var configs []struct {
				Symbol   string
				Leverage json.Number
			}
			if e := client.Request(ctx, http.MethodGet, "/fapi/v1/symbolConfig", url.Values{"symbol": {source.Symbol}}, true, &configs); e == nil && notional != nil {
				for _, config := range configs {
					if config.Symbol == source.Symbol {
						if leverage, e := positiveDecimal(config.Leverage.String(), "leverage"); e == nil {
							margin = new(big.Rat).Quo(notional, leverage)
						}
						break
					}
				}
			}
		}
		label := "本笔平仓对应保证金（估算）"
		if action == "open" {
			label = "本笔预估保证金"
		}
		body["message"] = fmt.Sprint(body["message"]) + "\n" + label + "：" + noticeNumber(margin) + " USDT"
		body["margin_checked"] = true
	}
	if !pnlChecked && (action == "close" || action == "unmatched_close") {
		asset := source.QuoteAsset
		if asset == "" {
			asset = "USDT"
		}
		var pnl *big.Rat
		if err == nil {
			pnl = exchange.RealizedPnL(ctx, client, source.Symbol, asset, result.OrderID, qty)
		}
		body["message"] = fmt.Sprint(body["message"]) + "\n本笔已实现盈亏（不含手续费）：" + noticeNumber(pnl) + " " + asset
		body["pnl_checked"] = true
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	raw, e := json.Marshal(body)
	if e != nil {
		return e
	}
	_, e = s.DB().ExecContext(ctx, "UPDATE events SET body=? WHERE id=?", string(raw), event)
	return e
}

func (s *Service) notificationMerge(ctx context.Context, target, channel, kind string, body map[string]any) (int64, string, string, bool, error) {
	if kind == "order" {
		key, err := RecordIdentity(body)
		if err != nil {
			return 0, "", "", false, err
		}
		rows, err := s.DB().QueryContext(ctx, "SELECT id,state,created FROM copy_tasks WHERE target_id=? AND order_key=?", target, key)
		if err != nil {
			return 0, "", "", false, err
		}
		ids := map[int64]bool{}
		var earliest int64
		pending := false
		for rows.Next() {
			var id, created int64
			var state string
			if err = rows.Scan(&id, &state, &created); err != nil {
				rows.Close()
				return 0, "", "", false, err
			}
			ids[id] = true
			if earliest == 0 || created < earliest {
				earliest = created
			}
			pending = pending || state == "pending" || state == "submitted"
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return 0, "", "", false, err
		}
		if len(ids) == 0 || time.Now().UnixMilli()-earliest >= 30000 {
			return 0, "", "", false, nil
		}
		if pending {
			return 0, "", "", true, nil
		}
		notices, err := s.DB().QueryContext(ctx, "SELECT e.body FROM events e JOIN outbox o ON o.event=e.id WHERE e.target=? AND e.kind='copy' AND e.stamp>=? AND o.channel=? AND o.status='pending'", target, earliest, channel)
		if err != nil {
			return 0, "", "", false, err
		}
		defer notices.Close()
		for notices.Next() {
			var raw string
			var notice struct {
				ID int64 `json:"copy_task_id"`
			}
			if err = notices.Scan(&raw); err != nil {
				return 0, "", "", false, err
			}
			if err = json.Unmarshal([]byte(raw), &notice); err != nil {
				return 0, "", "", false, err
			}
			if ids[notice.ID] {
				return 0, "", "", true, nil
			}
		}
		return 0, "", "", false, notices.Err()
	}
	if kind != "copy" || body["copy_task_id"] == nil {
		return 0, "", "", false, nil
	}
	var key string
	err := s.DB().QueryRowContext(ctx, "SELECT order_key FROM copy_tasks WHERE id=?", body["copy_task_id"]).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", "", false, nil
	}
	if err != nil {
		return 0, "", "", false, err
	}
	rows, err := s.DB().QueryContext(ctx, "SELECT o.id,e.name,e.body,e.stamp FROM events e JOIN outbox o ON o.event=e.id WHERE e.target=? AND e.kind='order' AND o.channel=? AND o.status='pending' ORDER BY e.id DESC", target, channel)
	if err != nil {
		return 0, "", "", false, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, stamp int64
		var name, raw string
		if err = rows.Scan(&id, &name, &raw, &stamp); err != nil {
			return 0, "", "", false, err
		}
		var leader map[string]any
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.UseNumber()
		if err = decoder.Decode(&leader); err != nil {
			return 0, "", "", false, err
		}
		candidate, err := RecordIdentity(leader)
		if err != nil {
			return 0, "", "", false, err
		}
		if candidate == key {
			title, message := notificationText(name, "order", stamp, leader)
			return id, title, message, false, nil
		}
	}
	return 0, "", "", false, rows.Err()
}
