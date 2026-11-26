package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSeverityRoutingAndAlertDeduplication(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "alerts.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.InitializeMonitorTables(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB().ExecContext(ctx, `INSERT OR REPLACE INTO settings(id,value) VALUES(1,'{"telegram_enabled":true,"telegram_min_severity":"critical","bark_enabled":false}')`); err != nil {
		t.Fatal(err)
	}
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := tx.ExecContext(ctx, "INSERT INTO events(target,name,kind,body,stamp,historical) VALUES('t','n','copy','{}',1,0)")
	infoID, _ := info.LastInsertId()
	critical, _ := tx.ExecContext(ctx, "INSERT INTO events(target,name,kind,body,stamp,historical) VALUES('t','n','copy','{}',2,0)")
	criticalID, _ := critical.LastInsertId()
	if err = QueueEventSeverity(ctx, tx, infoID, SeverityInfo); err != nil {
		t.Fatal(err)
	}
	if err = QueueEventSeverity(ctx, tx, criticalID, SeverityCritical); err != nil {
		t.Fatal(err)
	}
	if _, err = s.RaiseAlert(ctx, tx, AlertSpec{Key: "test:key", Scope: "account:1", Severity: SeverityCritical, Title: "风险", Message: "测试"}); err != nil {
		t.Fatal(err)
	}
	// A repeated evaluation inside the cooldown must not create another event.
	if _, err = s.RaiseAlert(ctx, tx, AlertSpec{Key: "test:key", Scope: "account:1", Severity: SeverityCritical, Title: "风险", Message: "测试"}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var outbox int
	if err = s.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox WHERE channel='telegram'").Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if outbox != 2 { // critical copy event + first alert only
		t.Fatalf("outbox %d", outbox)
	}
	var alertEvents, count int
	if err = s.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE kind='alert'").Scan(&alertEvents); err != nil {
		t.Fatal(err)
	}
	if err = s.DB().QueryRowContext(ctx, "SELECT count FROM alerts WHERE key='test:key' AND status='active'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if alertEvents != 1 || count != 2 {
		t.Fatalf("alert events=%d count=%d", alertEvents, count)
	}
}

func TestAccountHealthFailureCounterResetsOnSuccess(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "health.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.InitializeMonitorTables(ctx); err != nil {
		t.Fatal(err)
	}
	for want := 1; want <= 2; want++ {
		got, err := s.SaveAccountHealth(ctx, 1, false, false, false, "timeout")
		if err != nil || got != want {
			t.Fatalf("failures %d %v, want %d", got, err, want)
		}
	}
	got, err := s.SaveAccountHealth(ctx, 1, true, true, false, "")
	if err != nil || got != 0 {
		t.Fatalf("reset %d %v", got, err)
	}
}
