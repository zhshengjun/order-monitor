package monitor

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"order-monitor/internal/exchange"
	"order-monitor/internal/store"
)

func riskFixture(t *testing.T) *Service {
	t.Helper()
	s, err := OpenService(context.Background(), filepath.Join(t.TempDir(), "risk.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if _, err = s.DB().ExecContext(context.Background(), `INSERT INTO targets(id,name) VALUES('kol','KOL');
		INSERT INTO futures_accounts VALUES(1,'account','key','secret');
		INSERT INTO copy_bindings VALUES(1,'kol');
		INSERT INTO copy_live VALUES(1,'kol',1,0);
		INSERT INTO virtual_sessions VALUES(1,'kol',0,1);
		INSERT INTO copied_positions VALUES(1,'kol','BTCUSDT','LONG','1');
		INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,state,created)
		VALUES(1,'kol','p','{}','open','0','100','pending','pending',0)`); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestReconciliationOverAttributionTripsBreaker(t *testing.T) {
	s := riskFixture(t)
	ctx := context.Background()
	if err := s.reconcilePositions(ctx, 1, []exchange.FuturesPosition{{Symbol: "BTCUSDT", Side: "BOTH", Amount: "0.5"}}); err != nil {
		t.Fatal(err)
	}
	var status, active string
	if err := s.DB().QueryRowContext(ctx, "SELECT status FROM position_reconciliation WHERE account_id=1").Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRowContext(ctx, "SELECT active FROM risk_breakers WHERE scope='account:1'").Scan(&active); err != nil {
		t.Fatal(err)
	}
	var live int
	if err := s.DB().QueryRowContext(ctx, "SELECT enabled FROM copy_live WHERE account_id=1").Scan(&live); err != nil {
		t.Fatal(err)
	}
	var pending string
	if err := s.DB().QueryRowContext(ctx, "SELECT state FROM copy_tasks WHERE order_key='p'").Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if status != "over" || active != "1" || live != 0 || pending != "cancelled" {
		t.Fatalf("status=%s breaker=%s live=%d pending=%s", status, active, live, pending)
	}
	if _, err := s.DB().ExecContext(ctx, "UPDATE copied_positions SET quantity='0.5'"); err != nil {
		t.Fatal(err)
	}
	if err := s.reconcilePositions(ctx, 1, []exchange.FuturesPosition{{Symbol: "BTCUSDT", Side: "BOTH", Amount: "0.5"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRowContext(ctx, "SELECT status FROM position_reconciliation WHERE account_id=1").Scan(&status); err != nil || status != "match" {
		t.Fatalf("status after correction %s %v", status, err)
	}
	var activeAlerts int
	if err := s.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM alerts WHERE key LIKE 'reconcile:%' AND status='active'").Scan(&activeAlerts); err != nil || activeAlerts != 0 {
		t.Fatalf("active reconcile alerts %d %v", activeAlerts, err)
	}
	if err := s.ResetBreaker(ctx, "account:1"); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRowContext(ctx, "SELECT active FROM risk_breakers WHERE scope='account:1'").Scan(&active); err != nil || active != "0" {
		t.Fatalf("breaker after reset %s %v", active, err)
	}
	metrics, err := s.Metrics(ctx)
	if err != nil || !strings.Contains(metrics, "order_monitor_up 1") {
		t.Fatalf("metrics %v %q", err, metrics)
	}
}

func TestBreakerBlocksCopyEnableUntilReset(t *testing.T) {
	s := riskFixture(t)
	ctx := context.Background()
	if err := s.reconcilePositions(ctx, 1, []exchange.FuturesPosition{{Symbol: "BTCUSDT", Side: "BOTH", Amount: "0.5"}}); err != nil {
		t.Fatal(err)
	}
	enabled := true
	err := s.ApplyControl(ctx, "/api/copy-live", store.ControlInput{AccountID: int64Ptr(1), TargetID: "kol", Enabled: &enabled}, time.Now())
	if err == nil || !strings.Contains(err.Error(), "风控暂停") {
		t.Fatalf("copy enable while breaker active: %v", err)
	}
	if err := s.ResetBreaker(ctx, "account:1"); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyControl(ctx, "/api/copy-live", store.ControlInput{AccountID: int64Ptr(1), TargetID: "kol", Enabled: &enabled}, time.Now()); err != nil {
		t.Fatalf("copy enable after reset: %v", err)
	}
}

func int64Ptr(v int64) *int64 { return &v }
