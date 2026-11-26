package engine

import "testing"

func TestPlanOrderCopiesOpeningAndClosing(t *testing.T) {
	input := PlanInput{
		Key:    OrderKey{Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG"},
		Action: "open", Signal: Signal{"1", "100"},
		OppositePosition: "0", LeaderBalance: "100", WalletBalance: "100",
		Available: "10", Leverage: "10",
		Market: Market{"99.9", "100.1", "100", "0.001", "0.001", "1000", "5"},
	}
	order, err := PlanOrder(input)
	if err != nil || order.Quantity != "0.999" || order.ReduceOnly {
		t.Fatalf("open: %+v, %v", order, err)
	}
	input.Key.Side, input.Action = "SELL", "close"
	input.Signal.Quantity, input.LeaderBefore = "1", "2"
	input.ActualPosition, input.Attributed = "1", "0.301"
	order, err = PlanOrder(input)
	if err != nil || order.Quantity != "0.151" || !order.ReduceOnly {
		t.Fatalf("close in one-way mode: %+v, %v", order, err)
	}
	input.HedgeMode = true
	order, err = PlanOrder(input)
	if err != nil || order.PositionSide != "LONG" || order.ReduceOnly {
		t.Fatalf("close in hedge mode: %+v, %v", order, err)
	}
}
