package domain

import (
	"context"
	"errors"
	"testing"
	"time"
)

type memoryStore struct {
	fills   map[OrderKey]CumulativeFill
	changes []Comparison
	loads   int
	fail    bool
}

func (s *memoryStore) Load(_ context.Context, key OrderKey) (CumulativeFill, error) {
	s.loads++
	return s.fills[key], nil
}

func (s *memoryStore) Commit(_ context.Context, change Comparison) error {
	if s.fail || s.fills[change.Key] != change.Previous {
		return errors.New("commit failed")
	}
	s.fills[change.Key] = change.Fill
	s.changes = append(s.changes, change)
	return nil
}

func TestComparisonRecoversFromEvictionAndRestart(t *testing.T) {
	store := &memoryStore{fills: make(map[OrderKey]CumulativeFill)}
	ctx := context.Background()
	cache, err := NewComparator(store, 1)
	if err != nil {
		t.Fatal(err)
	}
	order := Observation{OrderKey{"KOL", "BTCUSDT", "BUY", "LONG", 100}, CumulativeFill{"0.1", "10"}}
	other := order
	other.Key.UpdateTime++
	for _, item := range []Observation{order, order, other, order} {
		if err := cache.Apply(ctx, item); err != nil {
			t.Fatal(err)
		}
	}
	if store.loads != 3 || len(store.changes) != 2 {
		t.Fatalf("got %d reads and %d committed changes", store.loads, len(store.changes))
	}

	restarted, err := NewComparator(store, 1)
	if err != nil {
		t.Fatal(err)
	}
	order.Fill = CumulativeFill{"0.25", "24"}
	store.fail = true
	if err := restarted.Apply(ctx, order); err == nil {
		t.Fatal("expected failed commit")
	}
	store.fail = false
	if err := restarted.Apply(ctx, order); err != nil {
		t.Fatal(err)
	}
	change := store.changes[2]
	if change.QuantityDelta != "0.15" || change.CostDelta != "14" {
		t.Fatalf("incorrect recovered delta: %+v", change)
	}
}

func TestPollSuccessDoesNotUseBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	err := Poll(ctx, func(context.Context) error {
		calls++
		if calls == 3 {
			cancel()
		}
		return nil
	}, func(error) time.Duration {
		t.Fatal("successful request must not trigger backoff")
		return 0
	})
	if calls != 3 || !errors.Is(err, context.Canceled) {
		t.Fatalf("got %d calls and %v", calls, err)
	}
}

func TestRegressionAcceptancePreventsRepeatedStop(t *testing.T) {
	store := &memoryStore{fills: make(map[OrderKey]CumulativeFill)}
	ctx := context.Background()
	cache, err := NewComparator(store, 4)
	if err != nil {
		t.Fatal(err)
	}
	key := OrderKey{Portfolio: "KOL", Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", UpdateTime: 100}
	large := Observation{Key: key, Fill: CumulativeFill{Quantity: "2", Cost: "200"}}
	if err := cache.Apply(ctx, large); err != nil {
		t.Fatal(err)
	}
	small := Observation{Key: key, Fill: CumulativeFill{Quantity: "1", Cost: "100"}}
	if err := cache.Apply(ctx, small); !errors.Is(err, ErrFillRegression) {
		t.Fatalf("expected regression, got %v", err)
	}
	if err := cache.Accept(ctx, small, "", map[string]any{"symbol": "BTCUSDT"}); err != nil {
		t.Fatal(err)
	}
	if got := store.fills[key]; got != small.Fill {
		t.Fatalf("accepted baseline %+v", got)
	}
	if err := cache.Apply(ctx, small); err != nil {
		t.Fatalf("accepted fill still regresses: %v", err)
	}
	grown := Observation{Key: key, Fill: CumulativeFill{Quantity: "1.5", Cost: "150"}}
	if err := cache.Apply(ctx, grown); err != nil {
		t.Fatal(err)
	}
	last := store.changes[len(store.changes)-1]
	if last.QuantityDelta != "0.5" || last.CostDelta != "50" {
		t.Fatalf("delta after acceptance %+v", last)
	}
}
