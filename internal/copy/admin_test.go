package copy

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeLookup struct {
	result OrderResult
	err    error
}

func (f fakeLookup) Order(context.Context, string, string) (OrderResult, error) {
	return f.result, f.err
}

func adminFixture(t *testing.T) *Service {
	t.Helper()
	s, err := OpenService(context.Background(), filepath.Join(t.TempDir(), "admin.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if _, err = s.DB().ExecContext(context.Background(), `INSERT INTO targets(id,name) VALUES('kol','KOL');
		INSERT INTO futures_accounts VALUES(1,'account','key','secret');
		INSERT INTO copy_bindings VALUES(1,'kol');
		INSERT INTO copy_live VALUES(1,'kol',1,0);
		INSERT INTO virtual_sessions VALUES(1,'kol',0,1);
		INSERT INTO virtual_trust VALUES(1,'kol',1,'test',0)`); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestResolveSubmittedAbandonsOnlyMissingOrder(t *testing.T) {
	s := adminFixture(t)
	ctx := context.Background()
	res, err := s.DB().ExecContext(ctx, `INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,state,quantity,created)
		VALUES(1,'kol','base','{"symbol":"BTCUSDT","side":"BUY","positionSide":"LONG"}','open','0','100','client','submitted','1',0)`)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	lookup := func(key, secret string) (OrderLookup, error) {
		return fakeLookup{err: ExchangeError{Status: 400, Code: -2013}}, nil
	}
	result, err := s.ResolveSubmittedCopy(ctx, id, lookup)
	if err != nil || result != "abandoned" {
		t.Fatalf("resolve %q %v", result, err)
	}
	if state := stringField(t, s, "SELECT state FROM copy_tasks WHERE id=?", id); state != "skipped" {
		t.Fatalf("state %s", state)
	}
	if !strings.Contains(stringField(t, s, "SELECT body FROM events WHERE kind='copy'"), "交易所无此订单") {
		t.Fatal("missing audit notice")
	}

	// A found non-terminal order must not be force-finished.
	if _, err = s.DB().ExecContext(ctx, "UPDATE copy_tasks SET state='submitted' WHERE id=?", id); err != nil {
		t.Fatal(err)
	}
	lookup = func(key, secret string) (OrderLookup, error) {
		return fakeLookup{result: OrderResult{Status: "NEW", Symbol: "BTCUSDT", ClientID: "client"}}, nil
	}
	if _, err = s.ResolveSubmittedCopy(ctx, id, lookup); err == nil {
		t.Fatal("non-terminal order must not be abandoned")
	}
}

func TestResolveSubmittedSettlesTerminalOrder(t *testing.T) {
	s := adminFixture(t)
	ctx := context.Background()
	res, err := s.DB().ExecContext(ctx, `INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,state,quantity,created)
		VALUES(1,'kol','base','{"symbol":"BTCUSDT","side":"BUY","positionSide":"LONG"}','open','0','100','client','submitted','1',0)`)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	lookup := func(key, secret string) (OrderLookup, error) {
		return fakeLookup{result: OrderResult{Status: "FILLED", Symbol: "BTCUSDT", ClientID: "client", OrderID: "9", Executed: "1"}}, nil
	}
	result, err := s.ResolveSubmittedCopy(ctx, id, lookup)
	if err != nil || result != "settled" {
		t.Fatalf("resolve %q %v", result, err)
	}
	if state := stringField(t, s, "SELECT state FROM copy_tasks WHERE id=?", id); state != "filled" {
		t.Fatalf("state %s", state)
	}
	if got := stringField(t, s, "SELECT quantity FROM copied_positions"); got != "1" {
		t.Fatalf("position %s", got)
	}
}

func TestAdjustCopyPositionRequiresNoInflightTasks(t *testing.T) {
	s := adminFixture(t)
	ctx := context.Background()
	in := PositionAdjustment{AccountID: 1, TargetID: "kol", Symbol: "BTCUSDT", PositionSide: "LONG", Quantity: "0.25"}
	if err := s.AdjustCopyPosition(ctx, in); err != nil {
		t.Fatal(err)
	}
	if got := stringField(t, s, "SELECT quantity FROM copied_positions"); got != "0.25" {
		t.Fatalf("position %s", got)
	}
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,state,created)
		VALUES(1,'kol','p','{}','open','0','100','c','pending',0)`); err != nil {
		t.Fatal(err)
	}
	in.Quantity = "0"
	if err := s.AdjustCopyPosition(ctx, in); err == nil || !strings.Contains(err.Error(), "不能修正") {
		t.Fatalf("inflight adjustment %v", err)
	}
}

func TestPartialFullCloseQueuesResidualClose(t *testing.T) {
	s := adminFixture(t)
	ctx := context.Background()
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO copied_positions VALUES(1,'kol','BTCUSDT','LONG','1');
		INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,state,quantity,created)
		VALUES(1,'kol','base','{"symbol":"BTCUSDT","side":"SELL","positionSide":"LONG","executedQty":"1"}','close','1','100','client','submitted','1',0);
		INSERT INTO copy_close_snapshots VALUES(1,'1','1',1)`); err != nil {
		t.Fatal(err)
	}
	if err := s.SettleCopy(ctx, 1, OrderResult{Status: "EXPIRED", Symbol: "BTCUSDT", ClientID: "client", Executed: "0.3", OrderID: "9"}); err != nil {
		t.Fatal(err)
	}
	if got := stringField(t, s, "SELECT quantity FROM copied_positions"); got != "0.7" {
		t.Fatalf("position %s", got)
	}
	var key, state, action string
	if err := s.DB().QueryRowContext(ctx, "SELECT order_key,state,action FROM copy_tasks WHERE id=2").Scan(&key, &state, &action); err != nil {
		t.Fatal(err)
	}
	if key != "base:rp1" || state != "pending" || action != "close" {
		t.Fatalf("residual task %s %s %s", key, state, action)
	}
	if !strings.Contains(stringField(t, s, "SELECT body FROM events WHERE kind='copy'"), "剩余 0.7 已重新排队") {
		t.Fatal("missing residual notice")
	}
}

func TestCloseWatchWithMissingTaskDoesNotBlockOthers(t *testing.T) {
	s := adminFixture(t)
	ctx := context.Background()
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,state,created)
		VALUES(1,'kol','base','{}','close','1','100','client','pending',?);
		INSERT INTO close_watch(account_id,target_id,symbol,position_side,order_key,task_id,state,next_check) VALUES(1,'kol','BTCUSDT','LONG','a',999,'pending',0);
		INSERT INTO close_watch(account_id,target_id,symbol,position_side,order_key,task_id,state,next_check) VALUES(1,'kol','ETHUSDT','LONG','b',1,'pending',0)`, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.checkCloseWatch(ctx, &http.Client{Timeout: time.Second}); !ok || err != nil {
		t.Fatalf("first watch %v %v", ok, err)
	}
	if got := stringField(t, s, "SELECT state FROM close_watch WHERE task_id=999"); got != "checked" {
		t.Fatalf("orphan watch state %s", got)
	}
	if _, err := s.checkCloseWatch(ctx, &http.Client{Timeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	var next float64
	if err := s.DB().QueryRowContext(ctx, "SELECT next_check FROM close_watch WHERE task_id=1").Scan(&next); err != nil || next == 0 {
		t.Fatalf("second watch was not rescheduled: %v %v", next, err)
	}
}

func TestUnmatchedCloseFallsBackToActualPosition(t *testing.T) {
	s := adminFixture(t)
	ctx := context.Background()
	if _, err := s.DB().ExecContext(ctx, "INSERT INTO copied_positions VALUES(1,'kol','BTCUSDT','LONG','1')"); err != nil {
		t.Fatal(err)
	}
	task := closeTask()
	task.Action = "unmatched_close"
	task.Body.Quantity = "2"
	task.Before = "2"
	client := fakeFutures(t, `[{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"1"}]`)
	prepared, err := s.preparePending(ctx, task, client, nil)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Order.Quantity != "1.000" || prepared.Close == nil || !prepared.Close.Full {
		t.Fatalf("unmatched close %+v", prepared)
	}
}

func TestZeroFillFullCloseAlsoQueuesResidual(t *testing.T) {
	s := adminFixture(t)
	ctx := context.Background()
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO copied_positions VALUES(1,'kol','BTCUSDT','LONG','1');
		INSERT INTO copy_tasks(account_id,target_id,order_key,body,action,before_qty,lead_balance,client_id,state,quantity,created)
		VALUES(1,'kol','base','{"symbol":"BTCUSDT","side":"SELL","positionSide":"LONG","executedQty":"1"}','close','1','100','client','submitted','1',0);
		INSERT INTO copy_close_snapshots VALUES(1,'1','1',1)`); err != nil {
		t.Fatal(err)
	}
	if err := s.SettleCopy(ctx, 1, OrderResult{Status: "EXPIRED", Symbol: "BTCUSDT", ClientID: "client", Executed: "0", OrderID: "9"}); err != nil {
		t.Fatal(err)
	}
	if got := stringField(t, s, "SELECT quantity FROM copied_positions"); got != "1" {
		t.Fatalf("position %s", got)
	}
	var state string
	if err := s.DB().QueryRowContext(ctx, "SELECT state FROM copy_tasks WHERE id=2").Scan(&state); err != nil || state != "pending" {
		t.Fatalf("residual task %s %v", state, err)
	}
}

func TestUnmatchedCloseRequiresTrustedVirtualPosition(t *testing.T) {
	s := adminFixture(t)
	ctx := context.Background()
	if _, err := s.DB().ExecContext(ctx, `UPDATE virtual_trust SET trusted=0,reason='历史缺失' WHERE account_id=1 AND target_id='kol';
		INSERT INTO copied_positions VALUES(1,'kol','BTCUSDT','LONG','1')`); err != nil {
		t.Fatal(err)
	}
	task := closeTask()
	task.Action = "unmatched_close"
	task.Body.Quantity = "2"
	task.Before = "2"
	client := fakeFutures(t, `[{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"1"}]`)
	if _, err := s.preparePending(ctx, task, client, nil); err == nil || !strings.Contains(err.Error(), "虚拟仓位不可信") {
		t.Fatalf("untrusted unmatched close %v", err)
	}
}
