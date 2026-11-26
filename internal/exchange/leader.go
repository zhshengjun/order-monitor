package exchange

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const leaderAPI = "/bapi/futures/v1/friendly/future/copy-trade/"

type LeaderClient struct {
	client      *http.Client
	base        string
	historyLock sync.Mutex
	historyNext time.Time
}

func NewLeaderClient(client *http.Client, base string) (*LeaderClient, error) {
	u, err := url.Parse(base)
	if client == nil || err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		return nil, errors.New("leader API requires an HTTP client and an HTTPS origin")
	}
	return &LeaderClient{client: client, base: base}, nil
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
	// The public copy-trade history endpoint is informal and rate sensitive;
	// keep at least two seconds between requests, including failures.
	if wait := time.Until(c.historyNext); wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return LeaderPage{}, ctx.Err()
		case <-timer.C:
		}
	}
	defer func() { c.historyNext = time.Now().Add(2 * time.Second) }()
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
