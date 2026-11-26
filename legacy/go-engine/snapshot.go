package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"
)

func (s *SQLiteStore) Snapshot(ctx context.Context, id int64, client *http.Client) (map[string]any, error) {
	var key, secret string
	err := s.db.QueryRowContext(ctx, "SELECT api_key,api_secret FROM futures_accounts WHERE id=?", id).Scan(&key, &secret)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, InputError("账号不存在")
	}
	if err != nil {
		return nil, err
	}
	c, err := NewFuturesClient(client, "https://fapi.binance.com", key, secret)
	if err != nil {
		return nil, err
	}
	var balances []map[string]any
	var positions []map[string]any
	var configs []struct {
		Symbol   string      `json:"symbol"`
		Leverage json.Number `json:"leverage"`
	}
	var balancesErr, positionsErr error
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		balancesErr = c.request(ctx, http.MethodGet, "/fapi/v3/balance", nil, true, &balances)
	}()
	go func() {
		defer wg.Done()
		positionsErr = c.request(ctx, http.MethodGet, "/fapi/v3/positionRisk", nil, true, &positions)
	}()
	go func() {
		defer wg.Done()
		_ = c.request(ctx, http.MethodGet, "/fapi/v1/symbolConfig", nil, true, &configs)
	}()
	wg.Wait()
	if balancesErr != nil {
		return nil, balancesErr
	}
	if positionsErr != nil {
		return nil, positionsErr
	}
	leverage := map[string]any{}
	for _, v := range configs {
		if n, e := v.Leverage.Int64(); e == nil && n > 0 {
			leverage[v.Symbol] = n
		}
	}
	for _, position := range positions {
		position["leverage"] = leverage[fmtString(position["symbol"])]
	}
	return map[string]any{"balances": balances, "positions": positions, "queried_at": time.Now().UnixMilli()}, nil
}
func fmtString(value any) string { v, _ := value.(string); return v }

func MarkPrices(ctx context.Context, client *http.Client) (map[string]any, error) {
	c := &FuturesClient{client: client, base: "https://fapi.binance.com"}
	var rows []struct {
		Symbol string `json:"symbol"`
		Price  string `json:"markPrice"`
	}
	if err := c.request(ctx, http.MethodGet, "/fapi/v1/premiumIndex", nil, false, &rows); err != nil {
		return map[string]any{"prices": map[string]string{}, "queried_at": nil}, nil
	}
	prices := map[string]string{}
	for _, row := range rows {
		if n, e := positiveDecimal(row.Price, "price"); e == nil && n.Sign() > 0 {
			prices[row.Symbol] = row.Price
		}
	}
	return map[string]any{"prices": prices, "queried_at": time.Now().UnixMilli()}, nil
}
func FuturesNetwork(ctx context.Context, client *http.Client) map[string]any {
	c := &FuturesClient{client: client, base: "https://fapi.binance.com"}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://fapi.binance.com", nil)
	proxy, _ := http.ProxyFromEnvironment(req)
	var result map[string]any
	err := c.request(ctx, http.MethodGet, "/fapi/v1/ping", url.Values{}, false, &result)
	output := map[string]any{"ok": err == nil, "proxy_enabled": proxy != nil}
	if err != nil {
		output["error"] = err.Error()
	}
	return output
}
