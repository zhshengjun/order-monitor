package copy

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSettleConfirmedFullCloseKeepsOtherLeader(t *testing.T) {
	ctx := context.Background()
	s, err := OpenService(ctx, filepath.Join(t.TempDir(), "monitor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.InitializeMonitorTables(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = s.DB().ExecContext(ctx, `INSERT INTO copied_positions VALUES(1,'one','BTCUSDT','LONG','3'),(1,'two','BTCUSDT','LONG','5');
	INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,state,quantity,created) VALUES(1,'one','a','{"symbol":"BTCUSDT","positionSide":"LONG"}','close','3','100','c','submitted','2',0);
	INSERT INTO copy_close_snapshots VALUES(1,'3','2',1)`)
	if err != nil {
		t.Fatal(err)
	}
	r := OrderResult{Status: "FILLED", Symbol: "BTCUSDT", ClientID: "c", Executed: "2", OrderID: "3"}
	for i := 0; i < 2; i++ {
		if err = s.SettleCopy(ctx, 1, r); err != nil {
			t.Fatal(err)
		}
	}
	for leader, want := range map[string]string{"one": "0", "two": "5"} {
		var got string
		if err = s.DB().QueryRowContext(ctx, "SELECT quantity FROM copied_positions WHERE target_id=?", leader).Scan(&got); err != nil || got != want {
			t.Fatalf("%s: %s, %v", leader, got, err)
		}
	}
}
