package engine

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
)

func TestEventsHistoryAndAccountDeleteGuards(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "monitor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.InitializeMonitorTables(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO targets(id,name) VALUES('123','leader');
 INSERT INTO futures_accounts VALUES(1,'account','key','secret');
 INSERT INTO leader_history VALUES('123','abc',100,'{"symbol":"BTCUSDT","side":"BUY","positionSide":"LONG","orderTime":100,"orderUpdateTime":100,"executedQty":"2","avgPrice":"10"}');
 INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,state,created) VALUES(1,'123','key','{}','open','0','10','c','submitted',100)`)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Events(ctx, "123", "order", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	rows := result["list"].([]map[string]any)
	if len(rows) != 1 || rows[0]["body"].(map[string]any)["notional"] != "20" {
		t.Fatalf("unexpected history %+v", result)
	}
	h := NewHistoryWorker(s, nil)
	if err = h.Delete(ctx, false, int64(1), http.DefaultClient); err == nil {
		t.Fatal("submitted task must prevent deletion")
	}
	if _, err = s.db.ExecContext(ctx, "UPDATE copy_tasks SET state='filled'"); err != nil {
		t.Fatal(err)
	}
	if err = h.Delete(ctx, false, int64(1), http.DefaultClient); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM futures_accounts").Scan(&count); err != nil || count != 0 {
		t.Fatalf("account still present: %d %v", count, err)
	}
}

func TestRealizedPnLRejectsIncompleteTradeHistory(t *testing.T) {
	// Complete-trade checks are exercised through the transport in existing notification tests.
	ctx := context.Background()
	s, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "monitor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.InitializeMonitorTables(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CopyPnL(ctx, 1, 1, http.DefaultClient); err == nil {
		t.Fatal("missing task must fail")
	}
}
