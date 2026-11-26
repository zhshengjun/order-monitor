package exchange

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"net/url"
	"strings"
)

type FuturesBalance struct {
	Asset     string `json:"asset"`
	Balance   string `json:"balance"`
	Available string `json:"availableBalance"`
}

type FuturesPosition struct {
	Symbol string `json:"symbol"`
	Side   string `json:"positionSide"`
	Amount string `json:"positionAmt"`
}

type SymbolRules struct {
	Symbol  string `json:"symbol"`
	Status  string `json:"status"`
	Filters []struct {
		Type        string `json:"filterType"`
		Step        string `json:"stepSize"`
		Minimum     string `json:"minQty"`
		Maximum     string `json:"maxQty"`
		Notional    string `json:"notional"`
		MinNotional string `json:"minNotional"`
	} `json:"filters"`
}

func (c *FuturesClient) Balances(ctx context.Context) ([]FuturesBalance, error) {
	var rows []FuturesBalance
	err := c.request(ctx, http.MethodGet, "/fapi/v3/balance", nil, true, &rows)
	return rows, err
}

func (c *FuturesClient) Positions(ctx context.Context) ([]FuturesPosition, error) {
	var rows []FuturesPosition
	err := c.request(ctx, http.MethodGet, "/fapi/v3/positionRisk", nil, true, &rows)
	return rows, err
}

func (c *FuturesClient) HedgeMode(ctx context.Context) (bool, error) {
	var mode struct {
		Hedged *bool `json:"dualSidePosition"`
	}
	if err := c.request(ctx, http.MethodGet, "/fapi/v1/positionSide/dual", nil, true, &mode); err != nil {
		return false, err
	}
	if mode.Hedged == nil {
		return false, errors.New("missing futures position mode")
	}
	return *mode.Hedged, nil
}

func (c *FuturesClient) MarketRules(ctx context.Context) ([]SymbolRules, error) {
	var exchange struct {
		Symbols []SymbolRules `json:"symbols"`
	}
	err := c.request(ctx, http.MethodGet, "/fapi/v1/exchangeInfo", nil, false, &exchange)
	return exchange.Symbols, err
}

func (c *FuturesClient) Market(ctx context.Context, symbol string, rules []SymbolRules, requireMark bool) (Market, error) {
	var market Market
	for _, rule := range rules {
		if rule.Symbol != symbol || rule.Status != "TRADING" {
			continue
		}
		market.MinNotional = "0"
		for _, filter := range rule.Filters {
			switch filter.Type {
			case "MARKET_LOT_SIZE":
				market.Step, market.MinQuantity, market.MaxQuantity = filter.Step, filter.Minimum, filter.Maximum
			case "MIN_NOTIONAL":
				market.MinNotional = filter.Notional
				if market.MinNotional == "" {
					market.MinNotional = filter.MinNotional
				}
			}
		}
		break
	}
	if market.Step == "" {
		return Market{}, errors.New("market quantity rules unavailable")
	}
	var book struct {
		Bid string `json:"bidPrice"`
		Ask string `json:"askPrice"`
	}
	if err := c.request(ctx, http.MethodGet, "/fapi/v1/ticker/bookTicker", url.Values{"symbol": {symbol}}, false, &book); err != nil {
		return Market{}, err
	}
	market.Bid, market.Ask = book.Bid, book.Ask
	if !requireMark {
		return market, nil
	}
	var mark struct {
		Symbol string `json:"symbol"`
		Price  string `json:"markPrice"`
	}
	if err := c.request(ctx, http.MethodGet, "/fapi/v1/premiumIndex", url.Values{"symbol": {symbol}}, false, &mark); err != nil {
		return Market{}, err
	}
	if mark.Symbol != "" && mark.Symbol != symbol {
		return Market{}, errors.New("标记价格响应交易对不一致")
	}
	market.Mark = mark.Price
	return market, nil
}

func PositionAmount(positions []FuturesPosition, symbol, side string) (string, error) {
	if side != "LONG" && side != "SHORT" {
		return "", errors.New("invalid position direction")
	}
	var quantity *big.Rat
	for _, position := range positions {
		if position.Symbol != symbol {
			continue
		}
		amount, err := decimal(strings.TrimPrefix(position.Amount, "-"))
		if err != nil {
			return "", errors.New("invalid position amount")
		}
		if strings.HasPrefix(position.Amount, "-") {
			amount.Neg(amount)
		}
		if amount.Sign() == 0 {
			continue
		}
		matches := position.Side == side || (position.Side == "BOTH" && (side == "LONG") == (amount.Sign() > 0))
		if !matches {
			continue
		}
		if quantity != nil {
			return "", errors.New("conflicting futures position modes")
		}
		quantity = amount.Abs(amount)
	}
	if quantity == nil {
		return "0", nil
	}
	scale, _ := quantity.FloatPrec()
	return quantity.FloatString(scale), nil
}
