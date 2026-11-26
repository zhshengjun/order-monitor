package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"order-monitor/internal/exchange"
	"order-monitor/internal/store"
	"strconv"
	"sync"
	"time"
)

type HistoryWorker struct {
	store  *Service
	source *exchange.LeaderClient
	mu     sync.Mutex
	active map[string]bool
}

func NewHistoryWorker(store *Service, source *exchange.LeaderClient) *HistoryWorker {
	return &HistoryWorker{store: store, source: source, active: map[string]bool{}}
}
func (h *HistoryWorker) Active(target string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.active[target]
}
func (h *HistoryWorker) Refresh(ctx context.Context, target string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	tx, err := h.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var enabled bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM targets WHERE id=? AND enabled=1)", target).Scan(&enabled); err != nil {
		return err
	}
	if !enabled {
		return InputError("请先开启该带单员的监听")
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM leader_history_stage WHERE target=?", target); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO leader_history_sync(target,page,complete,total,next_check,error,last_batch,failures) VALUES(?,1,0,0,0,'','',0) ON CONFLICT(target) DO UPDATE SET page=1,complete=0,total=0,next_check=0,error='',last_batch='',failures=0`, target); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	h.active[target] = true
	return nil
}

func (h *HistoryWorker) page(ctx context.Context, target string) error {
	var page, failures int
	var next float64
	var prior string
	err := h.store.DB().QueryRowContext(ctx, "SELECT page,next_check,failures,last_batch FROM leader_history_sync WHERE target=?", target).Scan(&page, &next, &failures, &prior)
	if err != nil {
		return err
	}
	if next > float64(time.Now().UnixMilli())/1000 {
		return nil
	}
	if page == 1 {
		tx, err := h.store.DB().BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM leader_history_stage WHERE target=?", target); err == nil {
			_, err = tx.ExecContext(ctx, "UPDATE leader_history_sync SET started_at=?,complete=0 WHERE target=?", time.Now().UnixMilli(), target)
		}
		if err != nil {
			tx.Rollback()
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	batch, err := h.source.History(ctx, target, page, 100)
	if err == nil {
		err = h.savePage(ctx, target, page, prior, batch)
	}
	if err != nil {
		failures++
		delay := min(900, 60*(1<<min(failures-1, 4)))
		_, saveErr := h.store.DB().ExecContext(ctx, "UPDATE leader_history_sync SET error=?,next_check=?,failures=? WHERE target=?", err.Error(), float64(time.Now().Unix())+float64(delay), failures, target)
		return errors.Join(err, saveErr)
	}
	if len(batch.Fills) == 0 {
		delete(h.active, target)
	}
	return nil
}
func (h *HistoryWorker) savePage(ctx context.Context, target string, page int, prior string, batch LeaderPage) error {
	total, err := strconv.Atoi(batch.Total.String())
	if err != nil || total < 0 {
		return errors.New("历史总数无效")
	}
	keys := make([]string, 0, len(batch.Fills))
	for _, fill := range batch.Fills {
		k, err := HistoryIdentity(fill)
		if err != nil {
			return err
		}
		keys = append(keys, k)
	}
	fingerprint := ""
	if len(keys) > 0 {
		raw, _ := json.Marshal(keys)
		sum := sha256.Sum256(raw)
		fingerprint = hex.EncodeToString(sum[:])
	}
	if page > 1 && fingerprint != "" && fingerprint == prior {
		return errors.New("上游连续返回相同的历史页面，请检查分页接口；同步未完成")
	}
	tx, err := h.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i, fill := range batch.Fills {
		raw, err := json.Marshal(fill)
		if err != nil {
			return err
		}
		stamp, err := strconv.ParseInt(fill.Created.String(), 10, 64)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO leader_history_stage VALUES(?,?,?,?)", target, keys[i], stamp, string(raw)); err != nil {
			return err
		}
	}
	complete := len(batch.Fills) == 0
	if complete {
		var started int64
		if err = tx.QueryRowContext(ctx, "SELECT started_at FROM leader_history_sync WHERE target=?", target).Scan(&started); err != nil {
			return err
		}
		rows, err := store.DashboardRows(ctx, tx, "SELECT body FROM records WHERE target=? AND kind='order' AND stamp>=?", target, started)
		if err != nil {
			return err
		}
		for _, row := range rows {
			raw := row["body"].(string)
			var fill LeaderFill
			if err = json.Unmarshal([]byte(raw), &fill); err != nil {
				return err
			}
			key, err := HistoryIdentity(fill)
			if err != nil {
				return err
			}
			stamp, err := strconv.ParseInt(fill.Created.String(), 10, 64)
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO leader_history_stage VALUES(?,?,?,?)", target, key, stamp, raw); err != nil {
				return err
			}
		}
		for _, query := range []string{"DELETE FROM leader_history WHERE target=?", "INSERT INTO leader_history SELECT * FROM leader_history_stage WHERE target=?", "DELETE FROM leader_history_stage WHERE target=?"} {
			if _, err = tx.ExecContext(ctx, query, target); err != nil {
				return err
			}
		}
	}
	next := page + 1
	delay := 5.0
	if complete {
		next = 1
		delay = 3600
	}
	now := float64(time.Now().UnixMilli()) / 1000
	_, err = tx.ExecContext(ctx, "UPDATE leader_history_sync SET page=?,complete=?,total=?,next_check=?,error='',updated=?,last_batch=?,failures=0 WHERE target=?", next, complete, total, now+delay, now, fingerprint, target)
	if err != nil {
		return err
	}
	return tx.Commit()
}
func (h *HistoryWorker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		h.mu.Lock()
		for target := range h.active {
			var enabled bool
			err := h.store.DB().QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM targets WHERE id=? AND enabled=1)", target).Scan(&enabled)
			if err != nil {
				fmt.Printf("history: %v\n", err)
			} else if !enabled {
				delete(h.active, target)
			} else if err = h.page(ctx, target); err != nil {
				fmt.Printf("history: %v\n", err)
			}
		}
		h.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
