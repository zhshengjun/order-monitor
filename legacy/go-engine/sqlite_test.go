package engine

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSQLiteStoreReopensAndPreservesFillWork(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "fills.db")
	open := func() (*SQLiteStore, *Comparator) {
		t.Helper()
		store, err := OpenSQLiteStore(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { store.Close() })
		cache, err := NewComparator(store, 1)
		if err != nil {
			t.Fatal(err)
		}
		return store, cache
	}
	order := Observation{OrderKey{"KOL", "BTCUSDT", "BUY", "LONG", 100}, CumulativeFill{"0.1", "10"}}
	store, cache := open()
	if err := cache.Apply(ctx, order); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, cache = open()
	if err := cache.Apply(ctx, order); err != nil {
		t.Fatal(err)
	}
	order.Fill = CumulativeFill{"0.2", "20"}
	if err := cache.Apply(ctx, order); err != nil {
		t.Fatal(err)
	}
	baseline, err := store.Load(ctx, order.Key)
	if err != nil || baseline != order.Fill {
		t.Fatalf("got baseline %+v: %v", baseline, err)
	}
	var count int
	err = store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM go_fill_work").Scan(&count)
	if err != nil || count != 2 {
		t.Fatalf("got %d durable fill changes: %v", count, err)
	}
}
