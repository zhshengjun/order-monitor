package exchange

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func newTestClient(t *testing.T, calls *int32) (*FuturesClient, func()) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(calls, 1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/fapi/v3/balance":
			fmt.Fprint(w, `[{"asset":"USDT","balance":"100","availableBalance":"90"}]`)
		case "/fapi/v3/positionRisk":
			fmt.Fprint(w, `[{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"1"}]`)
		case "/fapi/v1/positionSide/dual":
			fmt.Fprint(w, `{"dualSidePosition":false}`)
		case "/fapi/v1/exchangeInfo":
			fmt.Fprint(w, `{"symbols":[{"symbol":"BTCUSDT","status":"TRADING","filters":[{"filterType":"MARKET_LOT_SIZE","stepSize":"0.001","minQty":"0.001","maxQty":"1000"},{"filterType":"MIN_NOTIONAL","notional":"5"}]}]}`)
		case "/fapi/v1/ticker/bookTicker":
			fmt.Fprint(w, `{"bidPrice":"99.9","askPrice":"100.1"}`)
		case "/fapi/v1/premiumIndex":
			fmt.Fprint(w, `{"symbol":"BTCUSDT","markPrice":"100"}`)
		case "/fapi/v1/symbolConfig":
			fmt.Fprint(w, `[{"symbol":"BTCUSDT","leverage":10,"maxLeverage":125,"marginType":"CROSSED"}]`)
		default:
			http.NotFound(w, r)
		}
	}))
	client, err := NewFuturesClient(server.Client(), server.URL, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	return client, server.Close
}

func resetCaches() {
	hedgeModes.entries = map[string]cacheEntry[bool]{}
	marketRules.entries = map[string]cacheEntry[[]SymbolRules]{}
	leverages.entries = map[string]cacheEntry[LeverageInfo]{}
}

func TestSnapshotCachesMetadataButNotBalances(t *testing.T) {
	resetCaches()
	defer resetCaches()
	var calls int32
	client, closeServer := newTestClient(t, &calls)
	defer closeServer()
	ctx := context.Background()
	first, err := client.Snapshot(ctx, "BTCUSDT", true)
	if err != nil {
		t.Fatal(err)
	}
	if first.WalletBalance != "100" || first.Available != "90" || first.Leverage != "10" || first.Market.Ask != "100.1" || len(first.Positions) != 1 {
		t.Fatalf("snapshot %+v", first)
	}
	firstCalls := atomic.LoadInt32(&calls)
	if firstCalls < 6 {
		t.Fatalf("expected all endpoints on first snapshot, got %d", firstCalls)
	}
	if _, err = client.Snapshot(ctx, "BTCUSDT", true); err != nil {
		t.Fatal(err)
	}
	secondCalls := atomic.LoadInt32(&calls) - firstCalls
	if secondCalls != 4 { // balance, positions, bookTicker, premiumIndex
		t.Fatalf("expected 4 uncached calls on second snapshot, got %d", secondCalls)
	}
}

func TestCacheExpiryRefetches(t *testing.T) {
	resetCaches()
	defer resetCaches()
	var calls int32
	client, closeServer := newTestClient(t, &calls)
	defer closeServer()
	ctx := context.Background()
	if _, err := client.CachedHedgeMode(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CachedHedgeMode(ctx); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("cached mode calls %d", got)
	}
	hedgeModes.now = func() time.Time { return time.Now().Add(staticTTL + time.Second) }
	if _, err := client.CachedHedgeMode(ctx); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expired mode calls %d", got)
	}
}

func TestRiskComputesMaintenanceRatio(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fapi/v2/account" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, `{"totalMaintMargin":"48","totalCrossWalletBalance":"100","totalCrossUnPnl":"-20","multiAssetsMargin":false}`)
	}))
	defer server.Close()
	client, err := NewFuturesClient(server.Client(), server.URL, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	risk, err := client.Risk(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if risk.Ratio < 59.99 || risk.Ratio > 60.01 {
		t.Fatalf("ratio %f", risk.Ratio)
	}
	if risk.MultiAssets {
		t.Fatalf("risk %+v", risk)
	}
}

func TestRiskSkipsRatioInMultiAssetMode(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"totalMaintMargin":"48","totalWalletBalance":"100","totalUnrealizedProfit":"0","multiAssetsMargin":true}`)
	}))
	defer server.Close()
	client, err := NewFuturesClient(server.Client(), server.URL, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	risk, err := client.Risk(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if risk.Ratio != 0 || !risk.MultiAssets {
		t.Fatalf("risk %+v", risk)
	}
}
