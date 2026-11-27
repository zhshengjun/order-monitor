package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func stringField(t *testing.T, s *SQLiteStore, query string, args ...any) string {
	t.Helper()
	var value string
	if err := s.DB().QueryRowContext(context.Background(), query, args...).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestEnablingCopyRebasesVirtualPositionWithoutOrders(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "rebase.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.InitializeMonitorTables(ctx); err != nil {
		t.Fatal(err)
	}
	open := map[string]any{"symbol": "BTCUSDT", "side": "BUY", "positionSide": "LONG", "executedQty": "2", "avgPrice": "100", "orderTime": 100, "orderUpdateTime": 100}
	closeFill := map[string]any{"symbol": "BTCUSDT", "side": "SELL", "positionSide": "LONG", "executedQty": "0.5", "avgPrice": "110", "orderTime": 200, "orderUpdateTime": 200}
	rawOpen, _ := json.Marshal(open)
	rawClose, _ := json.Marshal(closeFill)
	if _, err = s.DB().ExecContext(ctx, `INSERT INTO targets(id,name) VALUES('kol','KOL');
		INSERT INTO futures_accounts VALUES(1,'account','key','secret');
		INSERT INTO copy_bindings VALUES(1,'kol');
		INSERT INTO leader_history_sync(target,page,complete,total) VALUES('kol',1,1,2);
		INSERT INTO leader_history VALUES('kol','a',100,?),('kol','b',200,?);
		INSERT INTO virtual_positions VALUES(1,'kol','BTCUSDT','LONG','9');
		INSERT INTO virtual_events VALUES(1,'kol','old','BTCUSDT','LONG',50,'open','1','0','1')`, string(rawOpen), string(rawClose)); err != nil {
		t.Fatal(err)
	}
	enabled := true
	if err = s.ApplyControl(ctx, "/api/copy-live", ControlInput{AccountID: ptrInt64(1), TargetID: "kol", Enabled: &enabled}, time.Now()); err != nil {
		t.Fatal(err)
	}
	var quantity string
	if err = s.DB().QueryRowContext(ctx, "SELECT quantity FROM virtual_positions WHERE account_id=1 AND target_id='kol'").Scan(&quantity); err != nil {
		t.Fatal(err)
	}
	if quantity != "1.5" {
		t.Fatalf("rebased position %s", quantity)
	}
	var tasks, events int
	if err = s.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM copy_tasks").Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if err = s.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM virtual_events").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if tasks != 0 || events != 0 {
		t.Fatalf("tasks=%d events=%d", tasks, events)
	}
	if !strings.Contains(stringField(t, s, "SELECT body FROM events WHERE kind='copy'"), "漏掉的开仓不补单") {
		t.Fatal("missing rebase notice")
	}
	var trusted bool
	if err = s.DB().QueryRowContext(ctx, "SELECT trusted FROM virtual_trust WHERE account_id=1 AND target_id='kol'").Scan(&trusted); err != nil || !trusted {
		t.Fatalf("trusted=%v err=%v", trusted, err)
	}
}

func ptrInt64(v int64) *int64 { return &v }

func TestEnablingCopyWithIncompleteHistoryMarksVirtualPositionUntrusted(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "untrusted.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.InitializeMonitorTables(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB().ExecContext(ctx, `INSERT INTO targets(id,name) VALUES('kol','KOL');
		INSERT INTO futures_accounts VALUES(1,'account','key','secret');
		INSERT INTO copy_bindings VALUES(1,'kol');
		INSERT INTO virtual_positions VALUES(1,'kol','BTCUSDT','LONG','9')`); err != nil {
		t.Fatal(err)
	}
	enabled := true
	if err = s.ApplyControl(ctx, "/api/copy-live", ControlInput{AccountID: ptrInt64(1), TargetID: "kol", Enabled: &enabled}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := stringField(t, s, "SELECT quantity FROM virtual_positions"); got != "9" {
		t.Fatalf("incomplete history must not overwrite position: %s", got)
	}
	var trusted bool
	var reason string
	if err = s.DB().QueryRowContext(ctx, "SELECT trusted,reason FROM virtual_trust WHERE account_id=1 AND target_id='kol'").Scan(&trusted, &reason); err != nil {
		t.Fatal(err)
	}
	if trusted || !strings.Contains(reason, "尚未回填") {
		t.Fatalf("trust=%v reason=%s", trusted, reason)
	}
}

func TestAddTargetStoresInitialBalance(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "add.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.InitializeMonitorTables(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB().ExecContext(ctx, "INSERT INTO targets(id,name,detail) VALUES('123','旧', '{}')"); err != nil {
		t.Fatal(err)
	}
	if err = s.SaveLeaderDetail(ctx, "123", "新名字", "987.65", []byte(`{"nickname":"新名字","marginBalance":"987.65"}`)); err != nil {
		t.Fatal(err)
	}
	var name, balance, detail string
	if err = s.DB().QueryRowContext(ctx, "SELECT name,balance,detail FROM targets WHERE id='123'").Scan(&name, &balance, &detail); err != nil {
		t.Fatal(err)
	}
	if name != "新名字" || balance != "987.65" || detail == "{}" {
		t.Fatalf("name=%s balance=%s detail=%s", name, balance, detail)
	}
}
