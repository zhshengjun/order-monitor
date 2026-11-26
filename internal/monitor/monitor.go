package monitor

import (
	"context"
	"log"
	"order-monitor/internal/exchange"
	"sync"
	"time"
)

type scheduledTarget struct {
	id                           string
	started                      float64
	initialized                  bool
	failures, interval, pageSize int
}

func (s *Service) pollTarget(ctx context.Context, poller LeaderPoller, target scheduledTarget) error {
	if _, err := s.DB().ExecContext(ctx, "INSERT OR IGNORE INTO monitor_start(target,started_at) VALUES(?,?)", target.id, int64(target.started)); err != nil {
		return err
	}
	err := poller.PollPortfolio(ctx, target.id, target.pageSize, time.UnixMilli(int64(target.started)), target.initialized)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	completed := float64(poller.Now().UnixMicro()) / 1e6
	if err != nil {
		delay := min(3600, max(target.interval, 1)*(1<<min(target.failures+1, 6)))
		if _, saveErr := s.DB().ExecContext(ctx, "UPDATE targets SET failures=failures+1,error=?,next_check=? WHERE id=? AND enabled=1", err.Error(), completed+float64(delay), target.id); saveErr != nil {
			return saveErr
		}
		log.Printf("带单员 %s 轮询失败，%d 秒后重试：%v", target.id, delay, err)
		return nil
	}
	_, err = s.DB().ExecContext(ctx, "UPDATE targets SET initialized=1,last_check=?,next_check=?,failures=0,error='' WHERE id=? AND enabled=1", completed, completed, target.id)
	return err
}

// RunLeaderMonitor dispatches queued fills and starts one polling loop per
// enabled leader. Each leader polls on its own schedule and keeps its own
// history pacing, so a slow leader cannot delay the others.
func (s *Service) RunLeaderMonitor(ctx context.Context, source *exchange.LeaderClient) {
	comparator, err := NewComparator(s, 4096)
	if err != nil {
		log.Print(err)
		return
	}
	dispatchCtx, stopDispatch := context.WithCancel(ctx)
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		for dispatchCtx.Err() == nil {
			dispatched, err := s.DispatchNext(dispatchCtx, time.Now())
			if err != nil && dispatchCtx.Err() == nil {
				log.Printf("订单监听任务失败：%v", err)
			}
			if dispatched && err == nil {
				continue
			}
			select {
			case <-dispatchCtx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()
	defer func() { stopDispatch(); <-dispatchDone }()
	running := map[string]bool{}
	var workers sync.WaitGroup
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	defer func() {
		stopDispatch()
		workers.Wait()
	}()
	for ctx.Err() == nil {
		s.startLeaderWorkers(ctx, source, comparator, running, &workers)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// startLeaderWorkers launches one goroutine per enabled leader that is not
// already running.
func (s *Service) startLeaderWorkers(ctx context.Context, source *exchange.LeaderClient, comparator *Comparator, running map[string]bool, workers *sync.WaitGroup) {
	targets, err := s.enabledTargets(ctx)
	if err != nil {
		log.Printf("读取带单员失败：%v", err)
		return
	}
	seen := make(map[string]bool, len(targets))
	for _, target := range targets {
		if target.id == "" {
			continue
		}
		seen[target.id] = true
		if running[target.id] {
			continue
		}
		running[target.id] = true
		workers.Add(1)
		go func(target scheduledTarget) {
			defer workers.Done()
			poller := LeaderPoller{Source: source, Store: s, Comparator: comparator, Now: time.Now}
			for ctx.Err() == nil {
				started := time.Now()
				if err := s.pollTarget(ctx, poller, target); err != nil && ctx.Err() == nil {
					log.Printf("带单员 %s 轮询异常：%v", target.id, err)
				}
				wait := time.Duration(target.interval) * time.Second
				if elapsed := time.Since(started); elapsed > wait {
					wait = time.Second
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(wait):
				}
			}
		}(target)
	}
	for id := range running {
		if !seen[id] {
			delete(running, id)
		}
	}
}

// enabledTargets lists every enabled leader with its schedule settings.
func (s *Service) enabledTargets(ctx context.Context) ([]scheduledTarget, error) {
	rows, err := s.DB().QueryContext(ctx, `SELECT t.id,COALESCE(m.started_at,(SELECT MIN(l.started_at) FROM copy_live l WHERE l.target_id=t.id AND l.enabled=1),?),
	t.initialized,t.failures,COALESCE(json_extract(settings.value,'$.interval'),5),COALESCE(json_extract(settings.value,'$.page_size'),20)
	FROM targets t LEFT JOIN monitor_start m ON m.target=t.id LEFT JOIN settings ON settings.id=1
	WHERE t.enabled=1 ORDER BY t.id`, float64(time.Now().UnixMilli())/1000)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var targets []scheduledTarget
	for rows.Next() {
		var target scheduledTarget
		if err = rows.Scan(&target.id, &target.started, &target.initialized, &target.failures, &target.interval, &target.pageSize); err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	return targets, rows.Err()
}
