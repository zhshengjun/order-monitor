package copy

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

type uncertainExchange struct {
	posts, queries int
	clientID       string
}

func (e *uncertainExchange) Submit(_ context.Context, _ MarketOrder, id string) (OrderResult, error) {
	e.posts++
	e.clientID = id
	return OrderResult{}, errors.New("response lost")
}

func (e *uncertainExchange) Order(_ context.Context, symbol, id string) (OrderResult, error) {
	e.queries++
	if e.clientID != id {
		return OrderResult{}, errors.New("queried a different order")
	}
	return OrderResult{
		OrderID: "123", Symbol: symbol, ClientID: id,
		Side: "SELL", PositionSide: "LONG", Status: "FILLED", Executed: "0.2",
	}, nil
}

func TestUnknownSubmissionReopensWithoutResending(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := OpenService(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	exchange := &uncertainExchange{}
	order := MarketOrder{Symbol: "BTCUSDT", Side: "SELL", PositionSide: "LONG", Quantity: "0.2"}
	first, err := store.Execute(ctx, exchange, "account/leader/fill", order)
	if err == nil || first.State != "submitted" {
		t.Fatalf("ambiguous response lost its pending state: %+v %v", first, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenService(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	second, err := store.Execute(ctx, exchange, first.TaskKey, MarketOrder{})
	if err != nil || second.State != "finished" || second.Order != order {
		t.Fatalf("reconciliation: %+v %v", second, err)
	}
	if exchange.posts != 1 || exchange.queries != 1 {
		t.Fatalf("got %d submissions and %d queries", exchange.posts, exchange.queries)
	}
}
