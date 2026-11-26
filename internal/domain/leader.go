package domain

import (
	"encoding/json"
	"errors"
	"math/big"
	"strconv"
)

type LeaderFill struct {
	Raw          json.RawMessage `json:"-"`
	Symbol       string          `json:"symbol"`
	Side         string          `json:"side"`
	PositionSide string          `json:"positionSide"`
	Type         string          `json:"type"`
	Quantity     json.Number     `json:"executedQty"`
	Price        json.Number     `json:"avgPrice"`
	Created      json.Number     `json:"orderTime"`
	Updated      json.Number     `json:"orderUpdateTime"`
	// Historic fields only exist on rows stored by older builds.
	Notional json.Number `json:"notional"`
	Pnl      json.Number `json:"totalPnl"`
}

// CumulativeNotional is the total quote amount for a stored row, preferring the
// merged notional and falling back to quantity times average price.
func (f LeaderFill) CumulativeNotional() (*big.Rat, error) {
	if f.Notional != "" {
		return Decimal(f.Notional.String())
	}
	quantity, err := Decimal(f.Quantity.String())
	if err != nil {
		return nil, err
	}
	price, err := Decimal(f.Price.String())
	if err != nil {
		return nil, err
	}
	return new(big.Rat).Mul(quantity, price), nil
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
	// Stored rows keep their original JSON (without merged fields); use the
	// struct form when the derived cumulative fields are present.
	if len(f.Raw) > 0 && f.Notional == "" && f.Pnl == "" && f.Type == "" {
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
	return "leader API failed: HTTP " + strconv.Itoa(e.Status) + ", Binance code " + e.Code
}

var ErrInvalidFillTime = errors.New("invalid leader fill time")

func (f LeaderFill) Observation(portfolio string) (Observation, error) {
	created, e1 := strconv.ParseInt(f.Created.String(), 10, 64)
	updated, e2 := strconv.ParseInt(f.Updated.String(), 10, 64)
	if e1 != nil || e2 != nil || updated < created || updated <= 0 {
		return Observation{}, ErrInvalidFillTime
	}
	quantity, err := PositiveDecimal(f.Quantity.String(), "leader fill quantity")
	if err != nil {
		return Observation{}, err
	}
	price, err := PositiveDecimal(f.Price.String(), "leader fill price")
	if err != nil {
		return Observation{}, err
	}
	cost := new(big.Rat).Mul(quantity, price)
	return Observation{
		Key:  OrderKey{Portfolio: portfolio, Symbol: f.Symbol, Side: f.Side, PositionSide: f.PositionSide, UpdateTime: updated},
		Fill: CumulativeFill{Quantity: f.Quantity.String(), Cost: DecimalString(cost)},
	}, nil
}
