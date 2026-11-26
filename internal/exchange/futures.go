package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type FuturesClient struct {
	client *http.Client
	base   string
	key    string
	secret string
	now    func() time.Time
}

func NewFuturesClient(client *http.Client, base, key, secret string) (*FuturesClient, error) {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		return nil, errors.New("futures base must be an HTTPS origin")
	}
	if client == nil || key == "" || secret == "" {
		return nil, errors.New("HTTP client and API credentials required")
	}
	transport := *client
	transport.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &FuturesClient{&transport, base, key, secret, time.Now}, nil
}

type ExchangeError struct {
	Status int
	Code   int
}

func (e ExchangeError) Error() string {
	return fmt.Sprintf("futures request failed: HTTP %d, Binance code %d", e.Status, e.Code)
}

func (e ExchangeError) DefinitelyRejected() bool {
	if e.Status != http.StatusBadRequest {
		return false
	}
	switch e.Code {
	case -1013, -1100, -1101, -1102, -1111, -2010, -2019, -2021, -2022, -2027, -2028:
		return true
	}
	return false
}

type OrderResult struct {
	Average      string      `json:"avgPrice"`
	OrderID      json.Number `json:"orderId"`
	ClientID     string      `json:"clientOrderId"`
	Symbol       string      `json:"symbol"`
	Side         string      `json:"side"`
	PositionSide string      `json:"positionSide"`
	Status       string      `json:"status"`
	Executed     string      `json:"executedQty"`
	Quote        string      `json:"cumQuote"`
}

func (c *FuturesClient) Submit(ctx context.Context, order MarketOrder, clientID string) (OrderResult, error) {
	if clientID == "" {
		return OrderResult{}, errors.New("client order ID required")
	}
	params := url.Values{
		"symbol": {order.Symbol}, "side": {order.Side}, "positionSide": {order.PositionSide},
		"quantity": {order.Quantity}, "type": {"MARKET"}, "newOrderRespType": {"RESULT"}, "newClientOrderId": {clientID},
	}
	if order.ReduceOnly {
		params.Set("reduceOnly", "true")
	}
	return c.orderRequest(ctx, http.MethodPost, params)
}

func (c *FuturesClient) Order(ctx context.Context, symbol, clientID string) (OrderResult, error) {
	if symbol == "" || clientID == "" {
		return OrderResult{}, errors.New("symbol and client order ID required")
	}
	return c.orderRequest(ctx, http.MethodGet, url.Values{"symbol": {symbol}, "origClientOrderId": {clientID}})
}

func (c *FuturesClient) orderRequest(ctx context.Context, method string, params url.Values) (OrderResult, error) {
	var result OrderResult
	if err := c.request(ctx, method, "/fapi/v1/order", params, true, &result); err != nil {
		return OrderResult{}, err
	}
	if result.OrderID == "" || result.Status == "" {
		return OrderResult{}, errors.New("invalid futures order response")
	}
	return result, nil
}

func (c *FuturesClient) request(ctx context.Context, method, path string, params url.Values, signed bool, dest any) error {
	if params == nil {
		params = make(url.Values)
	}
	if signed {
		params.Set("timestamp", strconv.FormatInt(c.now().UnixMilli(), 10))
		params.Set("recvWindow", "5000")
	}
	query := params.Encode()
	if signed {
		signature := hmac.New(sha256.New, []byte(c.secret))
		signature.Write([]byte(query))
		query += "&signature=" + hex.EncodeToString(signature.Sum(nil))
	}
	endpoint := c.base + path
	if query != "" {
		endpoint += "?" + query
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
	if err != nil {
		return errors.New("invalid futures request")
	}
	if signed {
		req.Header.Set("X-MBX-APIKEY", c.key)
	}
	response, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: futures transport failed", ErrNetwork)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(io.LimitReader(response.Body, 4_000_001))
	if err != nil || len(body) > 4_000_000 {
		return errors.New("invalid futures response length")
	}
	if response.StatusCode != http.StatusOK {
		failure := ExchangeError{Status: response.StatusCode}
		var payload struct {
			Code int `json:"code"`
		}
		if json.Unmarshal(body, &payload) == nil {
			failure.Code = payload.Code
		}
		return failure
	}
	if json.Unmarshal(body, dest) != nil {
		return errors.New("invalid futures response")
	}
	return nil
}

// FuturesRequestTimeout bounds one futures API call. A submit that times out is
// never resent; the task is reconciled by querying the original client order.
const FuturesRequestTimeout = 5 * time.Second

// FuturesSubmitTimeout bounds the order submission call. Reconciliation and
// other reads keep the shorter FuturesRequestTimeout.
const FuturesSubmitTimeout = 10 * time.Second
