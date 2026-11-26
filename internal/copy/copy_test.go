package copy

import (
	"context"
	"path/filepath"
	"testing"
)

type filledExchange struct {
	orders []MarketOrder
}

func (e *filledExchange) Submit(_ context.Context, order MarketOrder, id string) (OrderResult, error) {
	e.orders = append(e.orders, order)
	return OrderResult{OrderID: "123", ClientID: id, Symbol: order.Symbol, Side: order.Side, PositionSide: order.PositionSide, Status: "FILLED", Executed: order.Quantity}, nil
}

func (e *filledExchange) Order(context.Context, string, string) (OrderResult, error) {
	panic("finished order queried again")
}

func TestExecuteCopyOpenReduceAndClose(t *testing.T) {
	ctx := context.Background()
	store, err := OpenService(ctx, filepath.Join(t.TempDir(), "copy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	exchange := new(filledExchange)
	input := PlanInput{Key: OrderKey{Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG"}, Signal: Signal{Quantity: "1", Price: "100"}, LeaderBefore: "0", LeaderBalance: "100", WalletBalance: "100", Available: "100", Leverage: "10", OppositePosition: "0", Market: Market{Bid: "100", Ask: "100", Mark: "100", Step: "0.001", MinQuantity: "0.001", MaxQuantity: "1000", MinNotional: "5"}}
	for _, event := range []struct {
		id, side, before, qty, owned string
		reduce                       bool
	}{
		{"1", "BUY", "0", "1", "0", false},
		{"2", "SELL", "1", "0.3", "1", true},
		{"3", "SELL", "0.7", "0.7", "0.7", true},
	} {
		input.Key.Side = event.side
		input.LeaderBefore, input.Signal.Quantity = event.before, event.qty
		input.ActualPosition, input.Attributed = event.owned, event.owned
		got, err := store.ExecuteCopy(ctx, exchange, event.id, input)
		if err != nil || got.State != "finished" || got.Order.ReduceOnly != event.reduce {
			t.Fatalf("%s: %+v %v", event.id, got, err)
		}
		qty, _ := decimal(got.Order.Quantity)
		if decimalString(qty) != event.qty {
			t.Fatalf("wrong quantity %s", got.Order.Quantity)
		}
		if _, err := store.ExecuteCopy(ctx, exchange, event.id, PlanInput{}); err != nil {
			t.Fatal(err)
		}
	}
	if len(exchange.orders) != 3 {
		t.Fatalf("submitted %d orders", len(exchange.orders))
	}
}
