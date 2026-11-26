package exchange

import (
	"context"
	"encoding/json"
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

func RealizedPnL(ctx context.Context, c *FuturesClient, symbol, asset string, orderID json.Number, qty *big.Rat) *big.Rat {
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
