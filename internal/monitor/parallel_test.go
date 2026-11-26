package monitor

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestEnabledTargetsReadsSchedule(t *testing.T) {
	s, err := OpenService(context.Background(), filepath.Join(t.TempDir(), "par.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if _, err = s.DB().ExecContext(ctx, `INSERT INTO targets(id,name,enabled) VALUES('a','A',1),('b','B',1),('c','C',0);
		INSERT OR REPLACE INTO settings(id,value) VALUES(1,'{"interval":3,"page_size":20}')`); err != nil {
		t.Fatal(err)
	}
	targets, err := s.enabledTargets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 {
		t.Fatalf("targets %+v", targets)
	}
	for _, target := range targets {
		if target.interval != 3 || target.pageSize != 20 {
			t.Fatalf("schedule %+v", target)
		}
	}
}

func TestPollTargetFailureBacksOff(t *testing.T) {
	s, err := OpenService(context.Background(), filepath.Join(t.TempDir(), "backoff.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if _, err = s.DB().ExecContext(ctx, `INSERT INTO targets(id,name,enabled) VALUES('a','A',1)`); err != nil {
		t.Fatal(err)
	}
	cache, err := NewComparator(s, 4)
	if err != nil {
		t.Fatal(err)
	}
	poller := LeaderPoller{Source: testHistory{failPage: 1}, Store: s, Comparator: cache, Now: time.Now}
	target := scheduledTarget{id: "a", started: float64(time.Now().UnixMilli()), interval: 2, pageSize: 20}
	before := time.Now().Unix()
	if err := s.pollTarget(ctx, poller, target); err != nil {
		t.Fatal(err)
	}
	var failures int
	var next float64
	if err = s.DB().QueryRowContext(ctx, "SELECT failures,next_check FROM targets WHERE id='a'").Scan(&failures, &next); err != nil {
		t.Fatal(err)
	}
	if failures != 1 || next < float64(before+3) {
		t.Fatalf("failures=%d next=%f", failures, next)
	}
}
