package monitor

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestNotificationMergeWaitsForCopyAndMatchesOriginalOrder(t *testing.T) {
	ctx := context.Background()
	store, err := OpenService(ctx, filepath.Join(t.TempDir(), "monitor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.InitializeMonitorTables(ctx); err != nil {
		t.Fatal(err)
	}
	body := map[string]any{"symbol": "BTCUSDT", "side": "BUY", "positionSide": "LONG", "orderTime": int64(100), "orderUpdateTime": int64(100)}
	key, err := RecordIdentity(body)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.DB().ExecContext(ctx, `INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,state,created) VALUES(1,'kol',?,'{}','open','0','100','client','pending',?);
 INSERT INTO events(id,target,name,kind,body,stamp,historical) VALUES(1,'kol','KOL','order','{"symbol":"BTCUSDT","side":"BUY","positionSide":"LONG","orderTime":100,"orderUpdateTime":100}',100,0);
 INSERT INTO outbox(id,event,channel) VALUES(1,1,'bark')`, key, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, wait, err := store.notificationMerge(ctx, "kol", "bark", "order", body)
	if err != nil || !wait {
		t.Fatalf("pending copy should delay notification: %v %v", wait, err)
	}
	_, err = store.DB().ExecContext(ctx, "UPDATE copy_tasks SET state='filled' WHERE id=1")
	if err != nil {
		t.Fatal(err)
	}
	id, title, message, wait, err := store.notificationMerge(ctx, "kol", "bark", "copy", map[string]any{"copy_task_id": float64(1)})
	if err != nil || wait || id != 1 || title == "" || message == "" {
		t.Fatalf("merge: %d %q %q %v %v", id, title, message, wait, err)
	}
}

func TestMarginRiskDeduplicatesAtThreshold(t *testing.T) {
	no := false
	account := marginAccount{Multi: &no, Wallet: "100", Pnl: "0"}
	account.Positions = append(account.Positions, struct {
		Symbol   string `json:"symbol"`
		Side     string `json:"positionSide"`
		Isolated *bool  `json:"isolated"`
	}{"BTCUSDT", "LONG", &no})
	risks, err := marginRisks(account, []riskPosition{{Symbol: "BTCUSDT", Side: "LONG", Amount: "1", Maintenance: "60", Asset: "USDT"}})
	if err != nil || risks["cross:USDT"].maintenance.String() != "60/1" {
		t.Fatalf("risk: %+v %v", risks, err)
	}
}
