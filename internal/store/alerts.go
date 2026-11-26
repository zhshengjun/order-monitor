package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	SeverityInfo     = "info"
	SeverityWarning  = "warning"
	SeverityCritical = "critical"
)

// AlertSpec describes one deduplicated alert. The key must be stable.
type AlertSpec struct {
	Key      string
	Scope    string
	Severity string
	Title    string
	Message  string
}

func SeverityRank(value string) int {
	switch value {
	case SeverityCritical:
		return 3
	case SeverityWarning:
		return 2
	default:
		return 1
	}
}

func NormalizeSeverity(value, fallback string) string {
	switch value {
	case SeverityInfo, SeverityWarning, SeverityCritical:
		return value
	}
	if fallback == SeverityInfo || fallback == SeverityWarning || fallback == SeverityCritical {
		return fallback
	}
	return SeverityWarning
}

func alertCooldown(severity string) time.Duration {
	switch severity {
	case SeverityCritical:
		return 5 * time.Minute
	case SeverityWarning:
		return 15 * time.Minute
	default:
		return 60 * time.Minute
	}
}

// RaiseAlert upserts an active alert and emits a notification only when it is
// new, escalated, or the severity cooldown has elapsed. Repeated evaluations
// therefore deduplicate, while still re-notifying once per cooldown window.
func (s *SQLiteStore) RaiseAlert(ctx context.Context, tx *sql.Tx, spec AlertSpec) (bool, error) {
	severity := NormalizeSeverity(spec.Severity, SeverityWarning)
	now := time.Now()
	var status, previousSeverity string
	var firstSeen, lastNotified int64
	var count int
	err := tx.QueryRowContext(ctx, "SELECT status,severity,count,first_seen,last_notified FROM alerts WHERE key=?", spec.Key).Scan(&status, &previousSeverity, &count, &firstSeen, &lastNotified)
	exists := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	notify := !exists || status != "active" || SeverityRank(severity) > SeverityRank(previousSeverity)
	if exists && !notify {
		notify = now.Sub(time.UnixMilli(lastNotified)) >= alertCooldown(severity)
	}
	if !exists {
		firstSeen = now.UnixMilli()
		count = 0
	}
	count++
	body := map[string]any{"severity": severity, "alert_key": spec.Key, "scope": spec.Scope, "title": spec.Title, "message": spec.Message}
	if notify {
		duration := now.Sub(time.UnixMilli(firstSeen)).Round(time.Minute)
		body["message"] = fmt.Sprintf("%s（持续 %s，第 %d 次）", spec.Message, duration, count)
		lastNotified = now.UnixMilli()
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO alerts(key,scope,severity,title,message,status,count,first_seen,last_seen,last_notified)
		VALUES(?,?,?,?,?,'active',?,?,?,?)
		ON CONFLICT(key) DO UPDATE SET scope=excluded.scope,severity=excluded.severity,title=excluded.title,message=excluded.message,
		status='active',count=excluded.count,last_seen=excluded.last_seen,last_notified=excluded.last_notified`,
		spec.Key, spec.Scope, severity, spec.Title, spec.Message, count, firstSeen, now.UnixMilli(), lastNotified); err != nil {
		return false, err
	}
	if !notify {
		return false, nil
	}
	res, err := tx.ExecContext(ctx, "INSERT INTO events(target,name,kind,body,stamp,historical) VALUES(?,?,'alert',?,?,0)", spec.Scope, spec.Scope, string(raw), now.UnixMilli())
	if err != nil {
		return false, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return false, err
	}
	return true, QueueEventSeverity(ctx, tx, id, severity)
}

// ResolveAlert clears an active alert and emits a recovery notice once.
func (s *SQLiteStore) ResolveAlert(ctx context.Context, tx *sql.Tx, key, detail string) (bool, error) {
	res, err := tx.ExecContext(ctx, "UPDATE alerts SET status='resolved',last_seen=? WHERE key=? AND status='active'", time.Now().UnixMilli(), key)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return false, err
	}
	message := "已恢复正常"
	if strings.TrimSpace(detail) != "" {
		message = detail
	}
	body, _ := json.Marshal(map[string]any{"severity": SeverityInfo, "alert_key": key, "message": message})
	res, err = tx.ExecContext(ctx, "INSERT INTO events(target,name,kind,body,stamp,historical) VALUES('recovery','告警恢复','alert',?,?,0)", string(body), time.Now().UnixMilli())
	if err != nil {
		return false, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return false, err
	}
	return true, QueueEventSeverity(ctx, tx, id, SeverityInfo)
}

func (s *SQLiteStore) ActiveAlerts(ctx context.Context) ([]map[string]any, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := dashboardRows(ctx, tx, "SELECT key,scope,severity,title,message,count,first_seen,last_seen FROM alerts WHERE status='active' ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,last_seen DESC")
	if err != nil {
		return nil, err
	}
	return rows, tx.Commit()
}
