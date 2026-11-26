package exchange

import (
	"context"
	"net/http"
	"net/url"
	"time"
)

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
