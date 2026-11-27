package copy

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"order-monitor/internal/exchange"
)

type staleDetail struct{ balance string }

func (s staleDetail) Detail(_ context.Context, _ string) (exchange.LeaderDetail, error) {
	return exchange.LeaderDetail{ID: "kol", MarginBalance: json.Number(s.balance)}, nil
}

func leaderBalanceTask() PendingCopy {
	stamp := json.Number(fmt.Sprint(time.Now().UnixMilli()))
	task := PendingCopy{ID: 1, AccountID: 1, Target: "kol", Action: "open", LeaderBalance: "100", Created: time.Now().UnixMilli(), Multiplier: "1"}
	task.Body.LeaderFill = LeaderFill{Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", Quantity: "1", Price: "100", Created: stamp, Updated: stamp}
	return task
}

func TestRecentLeaderBalanceIsReusedAndStaleOneRefetched(t *testing.T) {
	ctx := context.Background()
	s, err := OpenService(ctx, filepath.Join(t.TempDir(), "lb.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.DB().ExecContext(ctx, `INSERT INTO targets(id,name,balance,balance_time) VALUES('kol','KOL','200',?)`, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	task := leaderBalanceTask()
	client := fakeFutures(t, `[{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"0"}]`)
	prepared, err := s.preparePending(ctx, task, client, staleDetail{"999"})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.LeaderBalance != "200" {
		t.Fatalf("recent saved balance should be reused, got %s", prepared.LeaderBalance)
	}
	if _, err = s.DB().ExecContext(ctx, "UPDATE targets SET balance_time=0"); err != nil {
		t.Fatal(err)
	}
	prepared, err = s.preparePending(ctx, task, client, staleDetail{"999"})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.LeaderBalance != "999" {
		t.Fatalf("stale saved balance should be refreshed, got %s", prepared.LeaderBalance)
	}
	var stored string
	var savedAt int64
	if err = s.DB().QueryRowContext(ctx, "SELECT balance,balance_time FROM targets WHERE id='kol'").Scan(&stored, &savedAt); err != nil {
		t.Fatal(err)
	}
	if stored != "999" || savedAt == 0 {
		t.Fatalf("balance=%s balance_time=%d", stored, savedAt)
	}
}
