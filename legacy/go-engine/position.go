package engine

import (
	"errors"
	"fmt"
	"math/big"
)

type PositionChange struct {
	Before string
	After  string
	Action string
}

// TransitionLeaderPosition mirrors the virtual position ledger used for
// proportional copies. An unmatched close leaves the virtual position intact.
func TransitionLeaderPosition(before, side, direction, executed string) (PositionChange, error) {
	current, err := decimal(before)
	if err != nil {
		return PositionChange{}, fmt.Errorf("invalid prior leader position: %w", err)
	}
	quantity, err := positiveDecimal(executed, "leader execution quantity")
	if err != nil {
		return PositionChange{}, err
	}
	change := PositionChange{Before: decimalString(current)}
	switch {
	case side == "BUY" && direction == "LONG", side == "SELL" && direction == "SHORT":
		change.Action = "open"
		current.Add(current, quantity)
	case side == "SELL" && direction == "LONG", side == "BUY" && direction == "SHORT":
		if current.Cmp(quantity) < 0 {
			change.Action = "unmatched_close"
		} else {
			change.Action = "close"
			current.Sub(current, quantity)
		}
	default:
		return PositionChange{}, errors.New("invalid leader order direction")
	}
	change.After = decimalString(current)
	return change, nil
}

func decimalString(value *big.Rat) string {
	scale, _ := value.FloatPrec()
	return value.FloatString(scale)
}
