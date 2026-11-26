package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/url"
)

type realizedTrade struct {
	ID       json.Number `json:"id"`
	OrderID  json.Number `json:"orderId"`
	Symbol   string      `json:"symbol"`
	Asset    string      `json:"marginAsset"`
	Quantity string      `json:"qty"`
	Pnl      string      `json:"realizedPnl"`
}

func realizedPnL(ctx context.Context, c *FuturesClient, symbol, asset string, orderID json.Number, qty *big.Rat) *big.Rat {
	if qty == nil || qty.Sign() <= 0 || orderID == "" {
		return nil
	}
	var trades []realizedTrade
	if err := c.request(ctx, http.MethodGet, "/fapi/v1/userTrades", url.Values{"symbol": {symbol}, "orderId": {orderID.String()}, "limit": {"1000"}}, true, &trades); err != nil || len(trades) == 0 {
		return nil
	}
	total, sum := new(big.Rat), new(big.Rat)
	seen := map[string]bool{}
	for _, trade := range trades {
		quantity, e1 := positiveDecimal(trade.Quantity, "trade quantity")
		profit, e2 := signedDecimal(trade.Pnl)
		if e1 != nil || e2 != nil || trade.ID == "" || seen[trade.ID.String()] || trade.Symbol != symbol || trade.OrderID != orderID || trade.Asset != "" && trade.Asset != asset {
			return nil
		}
		seen[trade.ID.String()] = true
		total.Add(total, quantity)
		sum.Add(sum, profit)
	}
	if total.Cmp(qty) != 0 {
		return nil
	}
	return sum
}
func (s *SQLiteStore) CopyPnL(ctx context.Context, account, task int64, httpClient *http.Client) (map[string]any, error) {
	var action, state, raw, key, secret string
	var resultRaw sql.NullString
	err := s.db.QueryRowContext(ctx, "SELECT c.action,c.state,c.body,c.result,a.api_key,a.api_secret FROM copy_tasks c JOIN futures_accounts a ON a.id=c.account_id WHERE c.id=? AND c.account_id=?", task, account).Scan(&action, &state, &raw, &resultRaw, &key, &secret)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, InputError("跟单记录不存在")
	}
	if err != nil {
		return nil, err
	}
	asset := "USDT"
	var pnl any
	if (action == "close" || action == "unmatched_close") && (state == "filled" || state == "finished") && resultRaw.Valid {
		var source struct {
			Symbol     string
			QuoteAsset string
		}
		var result OrderResult
		if json.Unmarshal([]byte(raw), &source) == nil && json.Unmarshal([]byte(resultRaw.String), &result) == nil {
			if source.QuoteAsset != "" {
				asset = source.QuoteAsset
			}
			qty, e := decimal(result.Executed)
			if e == nil {
				client, e := NewFuturesClient(httpClient, "https://fapi.binance.com", key, secret)
				if e == nil {
					if value := realizedPnL(ctx, client, source.Symbol, asset, result.OrderID, qty); value != nil {
						pnl = decimalString(value)
					}
				}
			}
		}
	}
	return map[string]any{"pnl": pnl, "asset": asset}, nil
}
