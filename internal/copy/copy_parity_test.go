package copy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openCopyStore(t *testing.T) *Service {
	t.Helper()
	store, err := OpenService(context.Background(), filepath.Join(t.TempDir(), "copy.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err = store.InitializeMonitorTables(context.Background()); err != nil {
		t.Fatal(err)
	}
	return store
}

// copyFixture creates one enabled leader binding whose copy switch is on.
func copyFixture(t *testing.T) (*Service, *Comparator, int64) {
	t.Helper()
	store := openCopyStore(t)
	ctx := context.Background()
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO targets(id,name) VALUES('kol','KOL');
		INSERT INTO futures_accounts VALUES(1,'account','key','secret');
		INSERT INTO copy_bindings VALUES(1,'kol');
		INSERT INTO copy_live VALUES(1,'kol',1,0);
		INSERT INTO virtual_sessions VALUES(1,'kol',0,1);`); err != nil {
		t.Fatal(err)
	}
	cache, err := NewComparator(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	return store, cache, time.Now().UnixMilli() - int64(time.Second/time.Millisecond)
}

func dispatch(t *testing.T, store *Service, cache *Comparator, obs Observation, balance string) {
	t.Helper()
	ctx := context.Background()
	if err := cache.Apply(ctx, obs, balance); err != nil {
		t.Fatal(err)
	}
	ok, err := store.DispatchNext(ctx, time.Now())
	if err != nil || !ok {
		t.Fatalf("dispatch: %v %v", ok, err)
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

func stringField(t *testing.T, store *Service, query string, args ...any) string {
	t.Helper()
	var value string
	if err := store.DB().QueryRowContext(context.Background(), query, args...).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestDispatchRepairsInactiveSessionAndQueuesFill(t *testing.T) {
	store, cache, stamp := copyFixture(t)
	ctx := context.Background()
	if _, err := store.DB().ExecContext(ctx, "UPDATE virtual_sessions SET active=0"); err != nil {
		t.Fatal(err)
	}
	observation := Observation{Key: OrderKey{Portfolio: "kol", Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", UpdateTime: stamp}, Fill: CumulativeFill{Quantity: "1", Cost: "100"}}
	dispatch(t, store, cache, observation, "100")

	if got := countRows(t, store, "SELECT active FROM virtual_sessions WHERE account_id=1 AND target_id='kol'"); got != 1 {
		t.Fatalf("session was not repaired: %d", got)
	}
	if got := countRows(t, store, "SELECT COUNT(*) FROM copy_tasks WHERE state='pending'"); got != 1 {
		t.Fatalf("expected one queued task, got %d", got)
	}
	if !strings.Contains(stringField(t, store, "SELECT body FROM events WHERE kind='copy'"), "跟单状态不一致") {
		t.Fatal("missing repair notice")
	}
}

// Python keeps tracking the virtual leader position for active sessions even
// while the copy switch is off; only the order submission is skipped.
func TestDispatchTracksVirtualLedgerWhenCopyDisabled(t *testing.T) {
	store, cache, stamp := copyFixture(t)
	ctx := context.Background()
	if _, err := store.DB().ExecContext(ctx, "UPDATE copy_live SET enabled=0"); err != nil {
		t.Fatal(err)
	}
	dispatch(t, store, cache, Observation{Key: OrderKey{Portfolio: "kol", Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", UpdateTime: stamp}, Fill: CumulativeFill{Quantity: "1", Cost: "100"}}, "100")
	dispatch(t, store, cache, Observation{Key: OrderKey{Portfolio: "kol", Symbol: "BTCUSDT", Side: "SELL", PositionSide: "LONG", UpdateTime: stamp + 1000}, Fill: CumulativeFill{Quantity: "0.4", Cost: "40"}}, "100")

	if got := countRows(t, store, "SELECT COUNT(*) FROM copy_tasks"); got != 0 {
		t.Fatalf("disabled copy queued %d tasks", got)
	}
	if got := stringField(t, store, "SELECT quantity FROM virtual_positions WHERE account_id=1 AND target_id='kol' AND symbol='BTCUSDT' AND position_side='LONG'"); got != "0.6" {
		t.Fatalf("virtual position %s", got)
	}
	if got := countRows(t, store, "SELECT COUNT(*) FROM virtual_events"); got != 2 {
		t.Fatalf("virtual events %d", got)
	}
}

func TestDispatchCreatesCloseWatchForFullAndUnmatchedClose(t *testing.T) {
	store, cache, stamp := copyFixture(t)
	ctx := context.Background()
	if _, err := store.DB().ExecContext(ctx, "INSERT INTO virtual_positions VALUES(1,'kol','BTCUSDT','LONG','1')"); err != nil {
		t.Fatal(err)
	}
	dispatch(t, store, cache, Observation{Key: OrderKey{Portfolio: "kol", Symbol: "BTCUSDT", Side: "SELL", PositionSide: "LONG", UpdateTime: stamp}, Fill: CumulativeFill{Quantity: "1", Cost: "100"}}, "100")
	if got := countRows(t, store, "SELECT COUNT(*) FROM close_watch WHERE task_id IS NOT NULL"); got != 1 {
		t.Fatalf("full close watch %d", got)
	}
	// Executed quantity above the tracked leader position is a close of unknown
	// previous size: Python still watches for a possible full close.
	if _, err := store.DB().ExecContext(ctx, "INSERT INTO virtual_positions VALUES(1,'kol','ETHUSDT','LONG','0.5')"); err != nil {
		t.Fatal(err)
	}
	dispatch(t, store, cache, Observation{Key: OrderKey{Portfolio: "kol", Symbol: "ETHUSDT", Side: "SELL", PositionSide: "LONG", UpdateTime: stamp + 1000}, Fill: CumulativeFill{Quantity: "2", Cost: "200"}}, "100")
	if got := countRows(t, store, "SELECT COUNT(*) FROM close_watch WHERE symbol='ETHUSDT'"); got != 1 {
		t.Fatalf("unmatched close watch %d", got)
	}
}

func TestDispatchIncrementReusesFirstMultiplierAndMarksStandalone(t *testing.T) {
	store, cache, stamp := copyFixture(t)
	ctx := context.Background()
	if _, err := store.DB().ExecContext(ctx, "INSERT INTO copy_multiplier_settings_v2 VALUES(1,'kol','2')"); err != nil {
		t.Fatal(err)
	}
	key := OrderKey{Portfolio: "kol", Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", UpdateTime: stamp}
	dispatch(t, store, cache, Observation{Key: key, Fill: CumulativeFill{Quantity: "1", Cost: "100"}}, "100")
	if _, err := store.DB().ExecContext(ctx, "UPDATE copy_multiplier_settings_v2 SET multiplier='3'"); err != nil {
		t.Fatal(err)
	}
	dispatch(t, store, cache, Observation{Key: key, Fill: CumulativeFill{Quantity: "2", Cost: "200"}}, "100")

	var body string
	if err := store.DB().QueryRowContext(ctx, "SELECT body FROM copy_tasks ORDER BY id DESC LIMIT 1").Scan(&body); err != nil {
		t.Fatal(err)
	}
	var signal map[string]any
	if err := json.Unmarshal([]byte(body), &signal); err != nil {
		t.Fatal(err)
	}
	if signal["executedQty"] != "1" || signal["incremental"] != true {
		t.Fatalf("increment signal %+v", signal)
	}
	if got := stringField(t, store, "SELECT multiplier FROM copy_task_multiplier_v2 ORDER BY task_id DESC LIMIT 1"); got != "2" {
		t.Fatalf("increment multiplier %s", got)
	}
	if got := stringField(t, store, "SELECT before_qty FROM copy_tasks ORDER BY id DESC LIMIT 1"); got != "1" {
		t.Fatalf("increment before_qty %s", got)
	}

	// A cumulative order first observed without any active session has no
	// followed base fill; the increment is standalone, like Python's
	// incremental_standalone flag.
	store2 := openCopyStore(t)
	target := time.Now().UnixMilli() - 1000
	if _, err := store2.DB().ExecContext(ctx, `INSERT INTO targets(id,name) VALUES('kol','KOL');
		INSERT INTO futures_accounts VALUES(1,'account','key','secret');
		INSERT INTO copy_bindings VALUES(1,'kol');
		INSERT INTO copy_live VALUES(1,'kol',0,0);`); err != nil {
		t.Fatal(err)
	}
	cache2, _ := NewComparator(store2, 8)
	if err := cache2.Apply(ctx, Observation{Key: OrderKey{Portfolio: "kol", Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", UpdateTime: target}, Fill: CumulativeFill{Quantity: "1", Cost: "100"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.DispatchNext(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.DB().ExecContext(ctx, "UPDATE copy_live SET enabled=1; INSERT INTO virtual_sessions VALUES(1,'kol',0,1)"); err != nil {
		t.Fatal(err)
	}
	dispatch(t, store2, cache2, Observation{Key: OrderKey{Portfolio: "kol", Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", UpdateTime: target}, Fill: CumulativeFill{Quantity: "2", Cost: "200"}}, "100")
	if err := store2.DB().QueryRowContext(ctx, "SELECT body FROM copy_tasks ORDER BY id DESC LIMIT 1").Scan(&body); err != nil {
		t.Fatal(err)
	}
	signal = nil
	if err := json.Unmarshal([]byte(body), &signal); err != nil {
		t.Fatal(err)
	}
	if signal["incremental_standalone"] != true {
		t.Fatalf("standalone increment signal %+v", signal)
	}
}

func TestDispatchLateFillStopsCopy(t *testing.T) {
	store, cache, stamp := copyFixture(t)
	dispatch(t, store, cache, Observation{Key: OrderKey{Portfolio: "kol", Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", UpdateTime: stamp}, Fill: CumulativeFill{Quantity: "1", Cost: "100"}}, "100")
	dispatch(t, store, cache, Observation{Key: OrderKey{Portfolio: "kol", Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", UpdateTime: stamp - 1000}, Fill: CumulativeFill{Quantity: "1", Cost: "100"}}, "100")

	if got := countRows(t, store, "SELECT COUNT(*) FROM copy_live WHERE enabled=1"); got != 0 {
		t.Fatal("copy switch still enabled")
	}
	if got := countRows(t, store, "SELECT COUNT(*) FROM copy_tasks WHERE state='pending'"); got != 0 {
		t.Fatal("pending task was not cancelled")
	}
	if !strings.Contains(stringField(t, store, "SELECT body FROM events WHERE kind='copy'"), "延迟到达") {
		t.Fatal("missing late fill notice")
	}
	if got := countRows(t, store, "SELECT COUNT(*) FROM virtual_trust WHERE trusted=1"); got != 0 {
		t.Fatal("late fill must mark virtual position untrusted")
	}
}

func TestFillRegressionStopsTheLeaderCopy(t *testing.T) {
	store, cache, stamp := copyFixture(t)
	ctx := context.Background()
	_ = ctx
	key := OrderKey{Portfolio: "kol", Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", UpdateTime: stamp}
	if err := cache.Apply(ctx, Observation{Key: key, Fill: CumulativeFill{Quantity: "2", Cost: "200"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DispatchNext(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	err := cache.Apply(ctx, Observation{Key: key, Fill: CumulativeFill{Quantity: "1", Cost: "100"}})
	if !errors.Is(err, ErrFillRegression) {
		t.Fatalf("regression error %v", err)
	}
	if err = store.StopCopyForRegression(ctx, Observation{Key: key, Fill: CumulativeFill{Quantity: "1", Cost: "100"}}); err != nil {
		t.Fatal(err)
	}
	if got := countRows(t, store, "SELECT COUNT(*) FROM copy_live WHERE enabled=1"); got != 0 {
		t.Fatal("copy switch still enabled after regression")
	}
	if got := countRows(t, store, "SELECT COUNT(*) FROM copy_tasks WHERE state='pending'"); got != 0 {
		t.Fatal("pending task was not cancelled after regression")
	}
	if got := countRows(t, store, "SELECT COUNT(*) FROM virtual_trust WHERE trusted=1"); got != 0 {
		t.Fatal("regression must mark virtual position untrusted")
	}
}

func TestSettleFullCloseShortageAndPartialFill(t *testing.T) {
	store := openCopyStore(t)
	ctx := context.Background()
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO copied_positions VALUES(1,'one','BTCUSDT','LONG','2');
		INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,state,quantity,created) VALUES(1,'one','a','{"symbol":"BTCUSDT","side":"SELL","positionSide":"LONG"}','close','2','100','c','submitted','2',0);
		INSERT INTO copy_close_snapshots VALUES(1,'2','1',1)`); err != nil {
		t.Fatal(err)
	}
	result := OrderResult{Status: "EXPIRED", Symbol: "BTCUSDT", ClientID: "c", Executed: "0.3", OrderID: "3"}
	if err := store.SettleCopy(ctx, 1, result); err != nil {
		t.Fatal(err)
	}
	if got := stringField(t, store, "SELECT quantity FROM copied_positions"); got != "0.7" {
		t.Fatalf("position after partial fill %s", got)
	}
	if got := stringField(t, store, "SELECT state FROM copy_tasks"); got != "finished" {
		t.Fatalf("state %s", got)
	}
}

func TestPlanOrderParityWithPythonCalculation(t *testing.T) {
	market := Market{Bid: "51000", Ask: "51000", Mark: "51000", Step: "0.00001", MinQuantity: "0.00001", MaxQuantity: "1000", MinNotional: "0"}
	input := PlanInput{Key: OrderKey{Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG"}, Action: "open", Signal: Signal{Quantity: "2", Price: "50000"}, LeaderBalance: "10000", WalletBalance: "100", Available: "1000", OppositePosition: "0", Leverage: "10", Market: market}
	for _, test := range []struct{ multiplier, want string }{{"1", "0.01960"}, {"2", "0.03921"}, {"0.5", "0.00980"}} {
		input.Multiplier = test.multiplier
		order, err := PlanOrder(input)
		if err != nil {
			t.Fatalf("multiplier %s: %v", test.multiplier, err)
		}
		if order.Quantity != test.want {
			t.Fatalf("multiplier %s: quantity %s, want %s", test.multiplier, order.Quantity, test.want)
		}
	}

	closeInput := PlanInput{Action: "close", Signal: Signal{Quantity: "1", Price: "100"}, LeaderBefore: "3", ActualPosition: "0.0105", Attributed: "0.0105", Market: Market{Bid: "99.9", Ask: "100.1", Mark: "100", Step: "0.001", MinQuantity: "0.001", MaxQuantity: "1000", MinNotional: "5"}}
	closeInput.Key = OrderKey{Symbol: "BTCUSDT", Side: "SELL", PositionSide: "LONG"}
	order, err := PlanOrder(closeInput)
	if err != nil || order.Quantity != "0.004" || !order.ReduceOnly {
		t.Fatalf("close rounds up: %+v %v", order, err)
	}
	closeInput.Signal.Quantity = "3"
	order, err = PlanOrder(closeInput)
	if err != nil || order.Quantity != "0.010" {
		t.Fatalf("full close cap: %+v %v", order, err)
	}
	closeInput.HedgeMode = true
	order, err = PlanOrder(closeInput)
	if err != nil || order.PositionSide != "LONG" || order.ReduceOnly {
		t.Fatalf("hedge close: %+v %v", order, err)
	}

	// Minimum notional tops a small copy up to the smallest valid order.
	small := PlanInput{Key: OrderKey{Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG"}, Action: "open", Signal: Signal{Quantity: "0.001", Price: "100"}, LeaderBalance: "1000", WalletBalance: "10", Available: "10", OppositePosition: "0", Leverage: "10", Market: Market{Bid: "100", Ask: "100", Mark: "100", Step: "0.001", MinQuantity: "0.001", MaxQuantity: "1000", MinNotional: "5"}}
	order, err = PlanOrder(small)
	if err != nil || order.Quantity != "0.050" {
		t.Fatalf("minimum notional: %+v %v", order, err)
	}
	small.Market.MaxQuantity = "0.01"
	if _, err = PlanOrder(small); err == nil {
		t.Fatal("minimum order above max quantity must be rejected")
	}

	// Book price more than 3% away from the leader fill is rejected.
	guarded := PlanInput{Key: OrderKey{Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG"}, Action: "open", Signal: Signal{Quantity: "1", Price: "100"}, LeaderBalance: "100", WalletBalance: "100", Available: "100", OppositePosition: "0", Leverage: "10", Market: Market{Bid: "100", Ask: "103.01", Mark: "100", Step: "0.001", MinQuantity: "0.001", MaxQuantity: "1000", MinNotional: "5"}}
	if _, err = PlanOrder(guarded); err == nil {
		t.Fatal("price guard did not trigger")
	}
}

// fakeFutures serves the read-only endpoints used by preparePending.
func fakeFutures(t *testing.T, positions string) *FuturesClient {
	t.Helper()
	return fakeFuturesWithBalance(t, positions, "10000")
}

func fakeFuturesWithBalance(t *testing.T, positions, balance string) *FuturesClient {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/fapi/v3/balance":
			fmt.Fprintf(w, `[{"asset":"USDT","balance":%q,"availableBalance":%q}]`, balance, balance)
		case "/fapi/v3/positionRisk":
			fmt.Fprint(w, positions)
		case "/fapi/v1/positionSide/dual":
			fmt.Fprint(w, `{"dualSidePosition":false}`)
		case "/fapi/v1/exchangeInfo":
			fmt.Fprint(w, `{"symbols":[{"symbol":"BTCUSDT","status":"TRADING","filters":[{"filterType":"MARKET_LOT_SIZE","stepSize":"0.001","minQty":"0.001","maxQty":"1000"},{"filterType":"MIN_NOTIONAL","notional":"5"}]}]}`)
		case "/fapi/v1/ticker/bookTicker":
			fmt.Fprint(w, `{"bidPrice":"99.9","askPrice":"100.1"}`)
		case "/fapi/v1/premiumIndex":
			fmt.Fprint(w, `{"symbol":"BTCUSDT","markPrice":"100"}`)
		case "/fapi/v1/symbolConfig":
			fmt.Fprint(w, `[{"symbol":"BTCUSDT","leverage":10}]`)
		default:
			http.NotFound(w, r)
		}
	})
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	client, err := NewFuturesClient(server.Client(), server.URL, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func closeTask() PendingCopy {
	stamp := json.Number(fmt.Sprint(time.Now().UnixMilli() - 1000))
	task := PendingCopy{ID: 1, AccountID: 1, Target: "kol", OriginalKey: "base", ClientID: "c", Action: "close", Before: "2", LeaderBalance: "100"}
	task.Body = struct {
		LeaderFill
		Incremental bool `json:"incremental"`
		Standalone  bool `json:"incremental_standalone"`
	}{LeaderFill: LeaderFill{Symbol: "BTCUSDT", Side: "SELL", PositionSide: "LONG", Quantity: "2", Price: "100", Created: stamp, Updated: stamp}}
	return task
}

func TestPrepareReductionProtectsOtherLeadersAndClearsEmptyOwner(t *testing.T) {
	store := openCopyStore(t)
	ctx := context.Background()
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO targets(id,name) VALUES('kol','KOL');
		INSERT INTO futures_accounts VALUES(1,'account','key','secret');
		INSERT INTO copied_positions VALUES(1,'kol','BTCUSDT','LONG','0.8'),(1,'other','BTCUSDT','LONG','0.6')`); err != nil {
		t.Fatal(err)
	}
	client := fakeFutures(t, `[{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"1"}]`)
	prepared, err := store.preparePending(ctx, closeTask(), client, nil)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Order.Quantity != "0.400" || prepared.Close == nil || prepared.Close.Actual != "0.4" || prepared.Close.Tracked != "0.8" || !prepared.Close.Full {
		t.Fatalf("unexpected prepared close %+v", prepared)
	}

	if _, err = store.DB().ExecContext(ctx, "UPDATE copied_positions SET quantity='1' WHERE target_id='other'"); err != nil {
		t.Fatal(err)
	}
	_, err = store.preparePending(ctx, closeTask(), client, nil)
	if !errors.Is(err, errNoRemainingPosition) {
		t.Fatalf("shortage error %v", err)
	}
}

func TestPrepareReductionRejectsPartialShortageAndReservations(t *testing.T) {
	store := openCopyStore(t)
	ctx := context.Background()
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO targets(id,name) VALUES('kol','KOL');
		INSERT INTO futures_accounts VALUES(1,'account','key','secret');
		INSERT INTO copied_positions VALUES(1,'kol','BTCUSDT','LONG','1'),(1,'other','BTCUSDT','LONG','1')`); err != nil {
		t.Fatal(err)
	}
	client := fakeFutures(t, `[{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"0.5"}]`)
	task := closeTask()
	task.Body.Quantity = "1"
	if _, err := store.preparePending(ctx, task, client, nil); err == nil || !strings.Contains(err.Error(), "合计超过") {
		t.Fatalf("partial shortage %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `DELETE FROM copied_positions WHERE target_id='other';
		INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,state,quantity,created) VALUES(1,'kol','older','{"symbol":"BTCUSDT","positionSide":"LONG"}','close','1','100','c2','submitted','0.3',0)`); err != nil {
		t.Fatal(err)
	}
	client = fakeFutures(t, `[{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"1"}]`)
	reservedTask := closeTask()
	reservedTask.Body.Quantity = "1"
	prepared, err := store.preparePending(ctx, reservedTask, client, nil)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Order.Quantity != "0.350" {
		t.Fatalf("reserved quantity not deducted: %+v", prepared)
	}
}

func TestPrepareIncrementStopsAtAlreadyFilledCumulativeTarget(t *testing.T) {
	store := openCopyStore(t)
	ctx := context.Background()
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO targets(id,name) VALUES('kol','KOL');
		INSERT INTO futures_accounts VALUES(1,'account','key','secret');
		INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,state,quantity,result,created) VALUES(1,'kol','base','{"symbol":"BTCUSDT"}','open','0','100','c','filled','0.05','{"executedQty":"0.05"}',0)`); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UnixMilli()
	key := OrderKey{Portfolio: "kol", Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", UpdateTime: stamp}
	if err := store.Commit(ctx, Comparison{Observation: Observation{Key: key, Fill: CumulativeFill{Quantity: "0.002", Cost: "0.2"}}}); err != nil {
		t.Fatal(err)
	}
	task := PendingCopy{ID: 2, AccountID: 1, Target: "kol", OriginalKey: "base", Action: "open", Before: "0.001", LeaderBalance: "100", Multiplier: "1", Created: time.Now().UnixMilli()}
	task.Body = struct {
		LeaderFill
		Incremental bool `json:"incremental"`
		Standalone  bool `json:"incremental_standalone"`
	}{LeaderFill: LeaderFill{Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", Quantity: "0.001", Price: "100", Created: json.Number(fmt.Sprint(stamp)), Updated: json.Number(fmt.Sprint(stamp))}, Incremental: true}
	client := fakeFuturesWithBalance(t, `[{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"0"}]`, "100")
	if _, err := store.preparePending(ctx, task, client, nil); err == nil || !strings.Contains(err.Error(), "累计跟单数量已达到目标") {
		t.Fatalf("expected cumulative target error, got %v", err)
	}
}

func TestInvalidFillWorkIsDeadLetteredWithoutBlocking(t *testing.T) {
	store, _, stamp := copyFixture(t)
	ctx := context.Background()
	poison, err := json.Marshal(Comparison{Observation: Observation{Key: OrderKey{Portfolio: "kol", Symbol: "BTCUSDT", Side: "BUY", PositionSide: "LONG", UpdateTime: stamp}, Fill: CumulativeFill{Quantity: "1", Cost: "100"}}, QuantityDelta: "0", CostDelta: "100"})
	if err != nil {
		t.Fatal(err)
	}
	valid, err := json.Marshal(Comparison{Observation: Observation{Key: OrderKey{Portfolio: "kol", Symbol: "ETHUSDT", Side: "BUY", PositionSide: "LONG", UpdateTime: stamp}, Fill: CumulativeFill{Quantity: "1", Cost: "100"}}, QuantityDelta: "1", CostDelta: "100"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB().ExecContext(ctx, "INSERT INTO go_fill_work(body) VALUES(?)", string(poison)); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB().ExecContext(ctx, "INSERT INTO go_fill_work(body) VALUES(?)", string(valid)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if ok, err := store.DispatchNext(ctx, time.Now()); !ok || err != nil {
			t.Fatalf("dispatch %d: %v %v", i, ok, err)
		}
	}
	if got := countRows(t, store, "SELECT COUNT(*) FROM go_fill_work"); got != 0 {
		t.Fatalf("work rows left %d", got)
	}
	if !strings.Contains(stringField(t, store, "SELECT body FROM events WHERE kind='copy'"), "跟单数据异常") {
		t.Fatal("missing dead-letter notice")
	}
}
