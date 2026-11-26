package monitor

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"order-monitor/internal/exchange"
	"time"
)

type scheduledTarget struct {
	id                           string
	started                      int64
	initialized                  bool
	failures, interval, pageSize int
}

func (s *Service) nextTarget(ctx context.Context, now time.Time) (scheduledTarget, bool, error) {
	var target scheduledTarget
	err := s.DB().QueryRowContext(ctx, `SELECT t.id,COALESCE(m.started_at,(SELECT MIN(l.started_at) FROM copy_live l WHERE l.target_id=t.id AND l.enabled=1),?),
	t.initialized,t.failures,COALESCE(json_extract(settings.value,'$.interval'),60),COALESCE(json_extract(settings.value,'$.page_size'),20)
	FROM targets t LEFT JOIN monitor_start m ON m.target=t.id LEFT JOIN settings ON settings.id=1
	WHERE t.enabled=1 AND t.next_check<=? ORDER BY t.next_check,t.id LIMIT 1`, now.UnixMilli(), float64(now.UnixMilli())/1000).Scan(&target.id, &target.started, &target.initialized, &target.failures, &target.interval, &target.pageSize)
	if errors.Is(err, sql.ErrNoRows) {
		return target, false, nil
	}
	return target, err == nil, err
}

func (s *Service) pollNext(ctx context.Context, poller LeaderPoller) (bool, error) {
	now := poller.Now()
	target, ok, err := s.nextTarget(ctx, now)
	if err != nil || !ok {
		return false, err
	}
	if _, err := s.DB().ExecContext(ctx, "INSERT OR IGNORE INTO monitor_start(target,started_at) VALUES(?,?)", target.id, target.started); err != nil {
		return false, err
	}
	err = poller.PollPortfolio(ctx, target.id, target.pageSize, time.UnixMilli(target.started), target.initialized)
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	completed := float64(poller.Now().UnixMicro()) / 1e6
	if err != nil {
		delay := min(3600, target.interval*(1<<min(target.failures+1, 6)))
		_, saveErr := s.DB().ExecContext(ctx, "UPDATE targets SET failures=failures+1,error=?,next_check=? WHERE id=? AND enabled=1", err.Error(), completed+float64(delay), target.id)
		if saveErr != nil {
			return false, saveErr
		}
		log.Printf("带单员 %s 轮询失败，%d 秒后重试：%v", target.id, delay, err)
		return true, nil
	}
	_, err = s.DB().ExecContext(ctx, "UPDATE targets SET initialized=1,last_check=?,next_check=?,failures=0,error='' WHERE id=? AND enabled=1", completed, completed, target.id)
	return true, err
}

// RunLeaderMonitor is the sole owner of the comparison cache. Both queued
// recovery and new observations use the same transactional dispatch path.
func (s *Service) RunLeaderMonitor(ctx context.Context, source *exchange.LeaderClient) {
	comparator, err := NewComparator(s, 4096)
	if err != nil {
		log.Print(err)
		return
	}
	poller := LeaderPoller{Source: source, Store: s, Comparator: comparator, Now: time.Now}
	for ctx.Err() == nil {
		dispatched, err := s.DispatchNext(ctx, time.Now())
		if err == nil && dispatched {
			continue
		}
		if err == nil {
			worked, pollErr := s.pollNext(ctx, poller)
			err = pollErr
			if worked && err == nil {
				continue
			}
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("订单监听任务失败：%v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
