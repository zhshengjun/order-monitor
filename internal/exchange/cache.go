package exchange

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"
)

// staticTTL is how long rarely changing exchange metadata is reused. Balances,
// positions and quotes are never cached.
const staticTTL = 5 * time.Minute

type cacheEntry[T any] struct {
	value   T
	expires time.Time
}

type ttlCache[T any] struct {
	mu      sync.Mutex
	entries map[string]cacheEntry[T]
	ttl     time.Duration
	now     func() time.Time
}

func newTTLCache[T any](ttl time.Duration) *ttlCache[T] {
	return &ttlCache[T]{entries: map[string]cacheEntry[T]{}, ttl: ttl, now: time.Now}
}

func (c *ttlCache[T]) get(key string) (T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok || c.now().After(entry.expires) {
		var zero T
		return zero, false
	}
	return entry.value, true
}

func (c *ttlCache[T]) put(key string, value T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = cacheEntry[T]{value: value, expires: c.now().Add(c.ttl)}
}

// CachedHedgeMode reuses the position mode for a few minutes; it changes only
// when the operator toggles the account setting.
func (c *FuturesClient) CachedHedgeMode(ctx context.Context) (bool, error) {
	if cached, ok := hedgeModes.get(c.key); ok {
		return cached, nil
	}
	mode, err := c.HedgeMode(ctx)
	if err != nil {
		return false, err
	}
	hedgeModes.put(c.key, mode)
	return mode, nil
}

// CachedMarketRules reuses exchange rules until TTL expiry.
func (c *FuturesClient) CachedMarketRules(ctx context.Context) ([]SymbolRules, error) {
	if cached, ok := marketRules.get("all"); ok {
		return cached, nil
	}
	rules, err := c.MarketRules(ctx)
	if err != nil {
		return nil, err
	}
	marketRules.put("all", rules)
	return rules, nil
}

// CachedLeverage reuses the symbol leverage for a few minutes.
func (c *FuturesClient) CachedLeverage(ctx context.Context, symbol string) (LeverageInfo, error) {
	key := c.key + "|" + symbol
	if cached, ok := leverages.get(key); ok {
		return cached, nil
	}
	var configs []struct {
		Symbol      string          `json:"symbol"`
		Leverage    json.RawMessage `json:"leverage"`
		MaxLeverage json.RawMessage `json:"maxLeverage"`
		MarginType  json.RawMessage `json:"marginType"`
	}
	if err := c.Request(ctx, http.MethodGet, "/fapi/v1/symbolConfig", urlValues(symbol), true, &configs); err != nil {
		return LeverageInfo{}, err
	}
	for _, config := range configs {
		if config.Symbol != symbol || len(config.Leverage) == 0 {
			continue
		}
		info := LeverageInfo{
			Leverage:    jsonNumberString(config.Leverage),
			MaxLeverage: jsonNumberString(config.MaxLeverage),
			MarginType:  string(config.MarginType),
		}
		leverages.put(key, info)
		return info, nil
	}
	return LeverageInfo{}, nil
}

func jsonNumberString(raw json.RawMessage) string {
	value := strings.Trim(strings.TrimSpace(string(raw)), `"`)
	return value
}

var (
	hedgeModes  = newTTLCache[bool](staticTTL)
	marketRules = newTTLCache[[]SymbolRules](staticTTL)
	leverages   = newTTLCache[LeverageInfo](staticTTL)
)

// LeverageInfo is the per-symbol leverage configuration used by preflight.
type LeverageInfo struct {
	Leverage    string
	MaxLeverage string
	MarginType  string
}
