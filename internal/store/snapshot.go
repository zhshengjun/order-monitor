package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"order-monitor/internal/exchange"
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
	c, err := exchange.NewFuturesClient(client, "https://fapi.binance.com", key, secret)
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
		balancesErr = c.Request(ctx, http.MethodGet, "/fapi/v3/balance", nil, true, &balances)
	}()
	go func() {
		defer wg.Done()
		positionsErr = c.Request(ctx, http.MethodGet, "/fapi/v3/positionRisk", nil, true, &positions)
	}()
	go func() {
		defer wg.Done()
		_ = c.Request(ctx, http.MethodGet, "/fapi/v1/symbolConfig", nil, true, &configs)
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
