package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"

	"order-monitor/internal/exchange"
)

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
				client, e := exchange.NewFuturesClient(httpClient, "https://fapi.binance.com", key, secret)
				if e == nil {
					if value := exchange.RealizedPnL(ctx, client, source.Symbol, asset, result.OrderID, qty); value != nil {
						pnl = decimalString(value)
					}
				}
			}
		}
	}
	return map[string]any{"pnl": pnl, "asset": asset}, nil
}
