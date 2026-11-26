package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

type testHistory struct {
	pages    [][]LeaderFill
	failPage int
}

func (h testHistory) History(_ context.Context, _ string, page, _ int) (LeaderPage, error) {
	if page == h.failPage {
		return LeaderPage{}, errors.New("upstream page unavailable")
	}
	if page > len(h.pages) {
		return LeaderPage{Fills: []LeaderFill{}, Total: "0"}, nil
	}
	return LeaderPage{Fills: h.pages[page-1], Total: "2"}, nil
}

func TestLeaderPollKeepsAutomaticOpenAndReduction(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1760000000000)
	makeFill := func(side string, age time.Duration) LeaderFill {
		stamp := json.Number(strconv.FormatInt(now.Add(-age).UnixMilli(), 10))
		return LeaderFill{Symbol: "BTCUSDT", Side: side, PositionSide: "LONG", Quantity: "1", Price: "100", Created: stamp, Updated: stamp}
	}
	open := makeFill("BUY", 2*time.Second)
	close := makeFill("SELL", time.Second)
	store, err := OpenService(ctx, filepath.Join(t.TempDir(), "fills.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.InitializeMonitorTables(ctx); err != nil {
		t.Fatal(err)
	}
	cache, err := NewComparator(store, 4)
	if err != nil {
		t.Fatal(err)
	}
	poller := LeaderPoller{Store: store, Comparator: cache, Now: func() time.Time { return now }}
	start := now.Add(-time.Minute)
	poller.Source = testHistory{pages: [][]LeaderFill{{close}, {open}}, failPage: 2}
	if err := poller.PollPortfolio(ctx, "leader", 1, start); err == nil {
		t.Fatal("expected page failure")
	}
	poller.Source = testHistory{pages: [][]LeaderFill{{close}, {open}}}
	for i := 0; i < 2; i++ {
		if err := poller.PollPortfolio(ctx, "leader", 1, start); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := store.DB().QueryContext(ctx, "SELECT body FROM go_fill_work ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var sides []string
	for rows.Next() {
		var body string
		var change Comparison
		if err := rows.Scan(&body); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(body), &change); err != nil {
			t.Fatal(err)
		}
		sides = append(sides, change.Key.Side)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(sides) != 2 || sides[0] != "BUY" || sides[1] != "SELL" {
		t.Fatalf("unexpected work order: %v", sides)
	}
}

func TestPollSkipsFillWithInvalidUpdateTimeAndWarns(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1760000000000)
	store, err := OpenService(ctx, filepath.Join(t.TempDir(), "invalid.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.DB().ExecContext(ctx, `INSERT INTO targets(id,name,initialized) VALUES('kol','KOL',1);
		INSERT INTO futures_accounts VALUES(1,'account','key','secret');
		INSERT INTO copy_live VALUES(1,'kol',1,0);
		INSERT INTO virtual_sessions VALUES(1,'kol',0,1);
		INSERT INTO virtual_positions VALUES(1,'kol','BTCUSDT','LONG','1')`); err != nil {
		t.Fatal(err)
	}
	cache, err := NewComparator(store, 4)
	if err != nil {
		t.Fatal(err)
	}
	stamp := json.Number("1760000000000")
	invalid := LeaderFill{Symbol: "BTCUSDT", Side: "SELL", PositionSide: "LONG", Quantity: "1", Price: "100", Created: stamp, Updated: ""}
	poller := LeaderPoller{Store: store, Comparator: cache, Now: func() time.Time { return now }, Source: testHistory{pages: [][]LeaderFill{{invalid}}}}
	if err := poller.PollPortfolio(ctx, "kol", 10, now.Add(-time.Minute), true); err != nil {
		t.Fatal(err)
	}
	if got := countRows(t, store, "SELECT COUNT(*) FROM go_fill_work"); got != 0 {
		t.Fatalf("invalid fill created %d work items", got)
	}
	if got := countRows(t, store, "SELECT COUNT(*) FROM events WHERE kind='copy'"); got != 1 {
		t.Fatalf("copy notices %d", got)
	}
	if got := countRows(t, store, "SELECT COUNT(*) FROM close_watch"); got != 1 {
		t.Fatalf("close watches %d", got)
	}
	if got := countRows(t, store, "SELECT COUNT(*) FROM virtual_trust WHERE trusted=0"); got != 1 {
		t.Fatalf("invalid fill must mark virtual position untrusted: %d", got)
	}
}

func countRows(t *testing.T, store *Service, query string, args ...any) int {
	t.Helper()
	var count int
	if err := store.DB().QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
