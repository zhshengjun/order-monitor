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
