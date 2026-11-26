package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"sync"
)

const leaderAPI = "/bapi/futures/v1/friendly/future/copy-trade/"

type LeaderClient struct {
	client      *http.Client
	base        string
	historyLock sync.Mutex
}

func NewLeaderClient(client *http.Client, base string) (*LeaderClient, error) {
	u, err := url.Parse(base)
	if client == nil || err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		return nil, errors.New("leader API requires an HTTP client and an HTTPS origin")
	}
	return &LeaderClient{client: client, base: base}, nil
}

type LeaderFill struct {
	Raw          json.RawMessage `json:"-"`
	Symbol       string          `json:"symbol"`
	Side         string          `json:"side"`
	PositionSide string          `json:"positionSide"`
	Quantity     json.Number     `json:"executedQty"`
	Price        json.Number     `json:"avgPrice"`
	Created      json.Number     `json:"orderTime"`
	Updated      json.Number     `json:"orderUpdateTime"`
}

func (f *LeaderFill) UnmarshalJSON(raw []byte) error {
	type fields LeaderFill
	var parsed fields
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return err
	}
	*f = LeaderFill(parsed)
	f.Raw = append(json.RawMessage(nil), raw...)
	return nil
}

func (f LeaderFill) MarshalJSON() ([]byte, error) {
	if len(f.Raw) > 0 {
		return f.Raw, nil
	}
	type fields LeaderFill
	return json.Marshal(fields(f))
}

type LeaderPage struct {
	Fills []LeaderFill `json:"list"`
	Total json.Number  `json:"total"`
}

type LeaderDetail struct {
	ID            string      `json:"leadPortfolioId"`
	Name          string      `json:"nickname"`
	MarginBalance json.Number `json:"marginBalance"`
}

type LeaderError struct {
	Status     int
	Code       string
	RetryAfter string
}

func (e LeaderError) Error() string {
	return fmt.Sprintf("leader API failed: HTTP %d, Binance code %s", e.Status, e.Code)
}

func (c *LeaderClient) Detail(ctx context.Context, portfolio string) (LeaderDetail, error) {
	var detail LeaderDetail
	query := url.Values{"portfolioId": {portfolio}}
	err := c.request(ctx, "lead-portfolio/detail?"+query.Encode(), nil, &detail)
	if err == nil && (detail.ID != portfolio || detail.MarginBalance == "") {
		err = errors.New("invalid leader portfolio detail")
	}
	return detail, err
}

func (c *LeaderClient) History(ctx context.Context, portfolio string, page, size int) (LeaderPage, error) {
	c.historyLock.Lock()
	defer c.historyLock.Unlock()
	var history LeaderPage
	if portfolio == "" || page < 1 || size < 1 || size > 100 {
		return history, errors.New("invalid leader history request")
	}
	body, err := json.Marshal(struct {
		Portfolio string `json:"portfolioId"`
		Page      int    `json:"pageNumber"`
		Size      int    `json:"pageSize"`
	}{portfolio, page, size})
	if err != nil {
		return history, err
	}
	err = c.request(ctx, "lead-portfolio/order-history", body, &history)
	if err == nil && (history.Fills == nil || history.Total == "") {
		err = errors.New("invalid leader history page")
	}
	return history, err
}

func (c *LeaderClient) request(ctx context.Context, path string, data []byte, dest any) error {
	method := http.MethodGet
	if data != nil {
		method = http.MethodPost
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+leaderAPI+path, bytes.NewReader(data))
	if err != nil {
		return errors.New("invalid leader API request")
	}
	req.Header.Set("Content-Type", "application/json")
	if path != "lead-portfolio/order-history" {
		req.Header.Set("clienttype", "web")
		req.Header.Set("User-Agent", "Mozilla/5.0 OrderMonitor/1.0")
	}
	response, err := c.client.Do(req)
	if err != nil {
		return errors.New("leader API transport failed")
	}
	defer response.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(response.Body, 8_000_000))
	if response.StatusCode != http.StatusOK {
		return LeaderError{Status: response.StatusCode, RetryAfter: response.Header.Get("Retry-After")}
	}
	var payload struct {
		Success bool            `json:"success"`
		Code    string          `json:"code"`
		Data    json.RawMessage `json:"data"`
	}
	if decoder.Decode(&payload) != nil || len(payload.Data) == 0 {
		return errors.New("invalid leader API response")
	}
	if !payload.Success || payload.Code != "000000" {
		return LeaderError{Status: response.StatusCode, Code: payload.Code}
	}
	return json.Unmarshal(payload.Data, dest)
}

func (f LeaderFill) Observation(portfolio string) (Observation, error) {
	created, e1 := strconv.ParseInt(f.Created.String(), 10, 64)
	updated, e2 := strconv.ParseInt(f.Updated.String(), 10, 64)
	if e1 != nil || e2 != nil || updated < created || updated <= 0 {
		return Observation{}, errors.New("invalid leader fill time")
	}
	quantity, err := positiveDecimal(f.Quantity.String(), "leader fill quantity")
	if err != nil {
		return Observation{}, err
	}
	price, err := positiveDecimal(f.Price.String(), "leader fill price")
	if err != nil {
		return Observation{}, err
	}
	cost := new(big.Rat).Mul(quantity, price)
	scale, _ := cost.FloatPrec()
	return Observation{
		Key:  OrderKey{Portfolio: portfolio, Symbol: f.Symbol, Side: f.Side, PositionSide: f.PositionSide, UpdateTime: updated},
		Fill: CumulativeFill{Quantity: f.Quantity.String(), Cost: cost.FloatString(scale)},
	}, nil
}
