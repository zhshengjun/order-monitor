package engine

import "testing"

func TestLeaderPositionTransitions(t *testing.T) {
	for _, test := range []struct {
		name, before, side, direction, quantity string
		want                                    PositionChange
	}{
		{"open long", "0.10", "BUY", "LONG", "0.22", PositionChange{"0.1", "0.32", "open"}},
		{"reduce long", "0.32", "SELL", "LONG", "0.11", PositionChange{"0.32", "0.21", "close"}},
		{"open short", "0", "SELL", "SHORT", "1", PositionChange{"0", "1", "open"}},
		{"close short", "1", "BUY", "SHORT", "1", PositionChange{"1", "0", "close"}},
		{"unmatched short close", "1", "BUY", "SHORT", "2", PositionChange{"1", "1", "unmatched_close"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := TransitionLeaderPosition(test.before, test.side, test.direction, test.quantity)
			if err != nil || got != test.want {
				t.Fatalf("got %+v, %v; want %+v", got, err, test.want)
			}
		})
	}
}
