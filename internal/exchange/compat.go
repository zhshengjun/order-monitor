package exchange

import (
	"context"
	"errors"
	"math/big"
	"net/url"

	"order-monitor/internal/domain"
)

var ErrNetwork = errors.New("预检查网络错误")

type (
	LeaderFill     = domain.LeaderFill
	LeaderPage     = domain.LeaderPage
	LeaderDetail   = domain.LeaderDetail
	LeaderError    = domain.LeaderError
	OrderKey       = domain.OrderKey
	Observation    = domain.Observation
	CumulativeFill = domain.CumulativeFill
	Market         = domain.Market
	MarketOrder    = domain.MarketOrder
	InputError     = domain.InputError
)

var (
	decimal         = domain.Decimal
	signedDecimal   = domain.SignedDecimal
	positiveDecimal = domain.PositiveDecimal
	decimalString   = domain.DecimalString
	decimalText     = domain.DecimalText
	noticeNumber    = domain.NoticeNumber
)

// Request exposes the signed/plain HTTP transport to persistence helpers in
// the same module without duplicating the signing logic.
func (c *FuturesClient) Request(ctx context.Context, method, path string, params url.Values, signed bool, dest any) error {
	return c.request(ctx, method, path, params, signed, dest)
}

func (c *LeaderClient) Request(ctx context.Context, path string, data []byte, dest any) error {
	return c.request(ctx, path, data, dest)
}

func ExecutionNotional(result OrderResult, filled *big.Rat) *big.Rat {
	if filled.Sign() == 0 {
		return new(big.Rat)
	}
	if q, err := positiveDecimal(result.Quote, "cumQuote"); err == nil {
		return q
	}
	if price, err := positiveDecimal(result.Average, "avgPrice"); err == nil {
		return price.Mul(price, filled)
	}
	return nil
}
