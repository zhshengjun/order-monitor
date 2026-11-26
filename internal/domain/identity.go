package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

func Direction(body map[string]any) string {
	side, _ := body["side"].(string)
	position, _ := body["positionSide"].(string)
	result := map[[2]string]string{{"BUY", "LONG"}: "开多", {"SELL", "LONG"}: "平多", {"SELL", "SHORT"}: "开空", {"BUY", "SHORT"}: "平空"}[[2]string{side, position}]
	if result == "" {
		return side
	}
	return result
}

// RecordIdentity groups leader order updates by update time and direction.
func RecordIdentity(body map[string]any) (string, error) {
	stamp, err := strconv.ParseInt(fmt.Sprint(body["orderUpdateTime"]), 10, 64)
	created, e := strconv.ParseInt(fmt.Sprint(body["orderTime"]), 10, 64)
	var identityTime any = stamp
	if err != nil || e != nil || stamp <= 0 || stamp < created {
		identityTime = []any{"invalid", body["orderTime"]}
	}
	parts := make([]any, 0, 4)
	for _, field := range []string{"symbol", "side", "positionSide"} {
		value, _ := body[field].(string)
		parts = append(parts, strings.ToUpper(strings.TrimSpace(value)))
	}
	encoded, err := json.Marshal(append(parts, identityTime))
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:]), nil
}

func HistoryIdentity(f LeaderFill) (string, error) {
	qty, err := Decimal(f.Quantity.String())
	if err != nil {
		return "", err
	}
	price, err := Decimal(f.Price.String())
	if err != nil {
		return "", err
	}
	stamp, err := strconv.ParseInt(f.Created.String(), 10, 64)
	if err != nil {
		return "", err
	}
	amount := new(big.Rat).Mul(qty, price)
	raw, err := json.Marshal([]any{f.Symbol, f.Side, f.PositionSide, stamp, DecimalString(qty), DecimalString(amount)})
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:]), nil
}
