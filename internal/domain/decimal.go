package domain

import (
	"errors"
	"math/big"
	"strings"
)

// Decimal parses an unsigned finite decimal exactly. The Binance API returns
// decimal strings; binary floating point is never used for order quantities.
func Decimal(value string) (*big.Rat, error) {
	for _, digit := range value {
		if digit != '.' && (digit < '0' || digit > '9') {
			return nil, errors.New("invalid decimal")
		}
	}
	parsed, ok := new(big.Rat).SetString(value)
	if !ok {
		return nil, errors.New("invalid decimal")
	}
	return parsed, nil
}

// SignedDecimal accepts one leading minus sign.
func SignedDecimal(value string) (*big.Rat, error) {
	if len(value) > 0 && value[0] == '-' {
		v, err := Decimal(value[1:])
		if err != nil {
			return nil, err
		}
		return v.Neg(v), nil
	}
	return Decimal(value)
}

func PositiveDecimal(value, name string) (*big.Rat, error) {
	n, err := Decimal(value)
	if err != nil || n.Sign() <= 0 {
		return nil, errors.New(name + " must be a positive decimal")
	}
	return n, nil
}

func DecimalString(value *big.Rat) string {
	scale, _ := value.FloatPrec()
	return value.FloatString(scale)
}

func DecimalText(value *big.Rat) string {
	if scale, exact := value.FloatPrec(); exact {
		return value.FloatString(scale)
	}
	text := strings.TrimRight(strings.TrimRight(value.FloatString(40), "0"), ".")
	if text == "" || text == "-" {
		return "0"
	}
	return text
}

// NoticeNumber formats at most four decimal places for display.
func NoticeNumber(n *big.Rat) string {
	if n == nil {
		return "—"
	}
	value := strings.TrimRight(strings.TrimRight(n.FloatString(4), "0"), ".")
	if value == "-0" {
		return "0"
	}
	return value
}

func Larger(a, b *big.Rat) *big.Rat {
	if a.Cmp(b) < 0 {
		return b
	}
	return a
}

// Lowercase aliases keep the ported call sites compact.
var (
	decimal         = Decimal
	signedDecimal   = SignedDecimal
	positiveDecimal = PositiveDecimal
	decimalString   = DecimalString
	decimalText     = DecimalText
	noticeNumber    = NoticeNumber
	larger          = Larger
)
