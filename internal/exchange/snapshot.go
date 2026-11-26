package exchange

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// PreflightSnapshot bundles the account state a copy task needs. The requests
// run concurrently; cached metadata is reused when fresh.
type PreflightSnapshot struct {
	Balances                 []FuturesBalance
	Positions                []FuturesPosition
	HedgeMode                bool
	Rules                    []SymbolRules
	Market                   Market
	Leverage                 string
	WalletBalance, Available string
	QueriedAt                int64
}

// Snapshot gathers balances, positions, position mode, market rules, the book
// ticker, and (for opens) the mark price and leverage in parallel.
func (c *FuturesClient) Snapshot(ctx context.Context, symbol string, requireMark bool) (PreflightSnapshot, error) {
	var snapshot PreflightSnapshot
	var balancesErr, positionsErr, modeErr, rulesErr, marketErr, leverageErr error
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		snapshot.Balances, balancesErr = c.Balances(ctx)
	}()
	go func() {
		defer wg.Done()
		snapshot.Positions, positionsErr = c.Positions(ctx)
	}()
	go func() {
		defer wg.Done()
		snapshot.HedgeMode, modeErr = c.CachedHedgeMode(ctx)
	}()
	go func() {
		defer wg.Done()
		snapshot.Rules, rulesErr = c.CachedMarketRules(ctx)
		if rulesErr != nil {
			return
		}
		snapshot.Market, marketErr = c.Market(ctx, symbol, snapshot.Rules, requireMark)
	}()
	if requireMark {
		wg.Add(1)
		go func() {
			defer wg.Done()
			info, err := c.CachedLeverage(ctx, symbol)
			leverageErr = err
			snapshot.Leverage = info.Leverage
			if leverageErr == nil && snapshot.Leverage == "" {
				leverageErr = errors.New("未获取到交易对杠杆配置")
			}
		}()
	}
	wg.Wait()
	for _, err := range []error{balancesErr, positionsErr, modeErr, rulesErr, marketErr, leverageErr} {
		if err != nil {
			return PreflightSnapshot{}, err
		}
	}
	for _, balance := range snapshot.Balances {
		if balance.Asset == "USDT" {
			snapshot.WalletBalance, snapshot.Available = balance.Balance, balance.Available
			break
		}
	}
	snapshot.QueriedAt = time.Now().UnixMilli()
	return snapshot, nil
}

// MarkPrices returns all futures mark prices for the leader position estimate.
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

// FuturesNetwork probes the futures API from the server container.
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

// AccountRisk is the account-level margin snapshot shown on the account card.
type AccountRisk struct {
	Ratio       float64
	Notional    string
	Leverage    string
	MultiAssets bool
}

// Risk snapshots /fapi/v2/account: total notional, maintenance margin and the
// cross/isolated margin balance. Multi-asset mode is reported separately
// because the margin ratio is undefined there.
func (c *FuturesClient) Risk(ctx context.Context) (AccountRisk, error) {
	var account struct {
		TotalMaintMargin        string `json:"totalMaintMargin"`
		TotalWalletBalance      string `json:"totalWalletBalance"`
		TotalUnrealizedProfit   string `json:"totalUnrealizedProfit"`
		TotalCrossWalletBalance string `json:"totalCrossWalletBalance"`
		TotalCrossUnPnl         string `json:"totalCrossUnPnl"`
		MultiAssetsMargin       bool   `json:"multiAssetsMargin"`
	}
	if err := c.Request(ctx, http.MethodGet, "/fapi/v2/account", nil, true, &account); err != nil {
		return AccountRisk{}, err
	}
	risk := AccountRisk{MultiAssets: account.MultiAssetsMargin}
	maint := new(big.Rat)
	if value, err := decimal(account.TotalMaintMargin); err == nil {
		maint = value
	}
	margin := new(big.Rat)
	wallet, e1 := signedDecimal(account.TotalCrossWalletBalance)
	pnl, e2 := signedDecimal(account.TotalCrossUnPnl)
	if e1 == nil && e2 == nil {
		margin.Add(wallet, pnl)
	} else if value, err := signedDecimal(account.TotalWalletBalance); err == nil {
		wallet, _ := signedDecimal(account.TotalUnrealizedProfit)
		if wallet != nil {
			margin.Add(value, wallet)
		} else {
			margin = value
		}
	}
	if margin.Sign() > 0 && !risk.MultiAssets {
		ratio, _ := new(big.Rat).Quo(maint, margin).Float64()
		risk.Ratio = ratio * 100
	}
	return risk, nil
}
