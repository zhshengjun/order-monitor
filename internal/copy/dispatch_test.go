package copy

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestDispatchPersistsAccountTasksAndFullCloseWatch(t *testing.T) {
	ctx := context.Background()
	store, err := OpenService(ctx, filepath.Join(t.TempDir(), "monitor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.InitializeMonitorTables(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = store.DB().ExecContext(ctx, `INSERT INTO targets(id,name) VALUES('9','leader');
	INSERT INTO copy_bindings VALUES(1,'9');
	INSERT INTO copy_live VALUES(1,'9',1,0);
	INSERT INTO virtual_sessions VALUES(1,'9',0,1);
	INSERT INTO copy_multiplier_settings_v2 VALUES(1,'9','2');`)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewComparator(store, 4)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for i, event := range []struct{ side, quantity, cost, action, before string }{
		{"BUY", "1", "100", "open", "0"},
		{"SELL", "0.4", "40", "close", "1"},
		{"SELL", "0.6", "60", "close", "0.6"},
	} {
		key := OrderKey{Portfolio: "9", Symbol: "BTCUSDT", Side: event.side, PositionSide: "LONG", UpdateTime: now.UnixMilli() + int64(i)}
		if err := cache.Apply(ctx, Observation{Key: key, Fill: CumulativeFill{Quantity: event.quantity, Cost: event.cost}}, "500"); err != nil {
			t.Fatal(err)
		}
		ok, err := store.DispatchNext(ctx, now)
		if !ok || err != nil {
			t.Fatalf("dispatch: %v", err)
		}
		var action, before, balance, multiplier string
		err = store.DB().QueryRowContext(ctx, `SELECT c.action,c.before_qty,c.lead_balance,m.multiplier FROM copy_tasks c JOIN copy_task_multiplier_v2 m ON m.task_id=c.id ORDER BY id DESC LIMIT 1`).Scan(&action, &before, &balance, &multiplier)
		if err != nil || action != event.action || before != event.before || balance != "500" || multiplier != "2" {
			t.Fatalf("task %d: %s %s %s %s %v", i, action, before, balance, multiplier, err)
		}
	}
	var watches int
	if err := store.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM close_watch").Scan(&watches); err != nil || watches != 1 {
		t.Fatalf("watches=%d: %v", watches, err)
	}
	if ok, err := store.DispatchNext(ctx, now); ok || err != nil {
		t.Fatalf("duplicate work: %v %v", ok, err)
	}
}
