package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"

	"order-monitor/internal/domain"
)

// sameBatchWindow groups leader fills and copy executions that belong to the
// same exchange order and arrived within a few seconds of each other.
const sameBatchWindowMS = 15000

// combineBatch folds pending notifications of the same exchange order (same
// orderTime) into one message, keeping only the latest cumulative state.
// Returns the extra outbox IDs that were absorbed.
func (s *Service) combineBatch(ctx context.Context, event int64, channel, kind, bodyRaw string, body map[string]any) (string, []int64, error) {
	if kind != "order" && kind != "copy" {
		return bodyRaw, nil, nil
	}
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return bodyRaw, nil, err
	}
	defer tx.Rollback()
	base, err := s.batchBaseKey(ctx, tx, kind, body)
	if err != nil || base == "" {
		return bodyRaw, nil, err
	}
	tx.Rollback()
	stamp := int64(0)
	if raw, ok := body["orderUpdateTime"]; ok {
		if n, e := jsonNumber(raw); e == nil {
			stamp = n
		}
	}
	if raw, ok := body["orderTime"]; ok && stamp == 0 {
		if n, e := jsonNumber(raw); e == nil {
			stamp = n
		}
	}
	switch kind {
	case "order":
		return s.combineLeaderOrder(ctx, base, event, bodyRaw, body, stamp)
	default:
		return s.combineCopyTasks(base, event, bodyRaw, channel)
	}
}

func jsonNumber(value any) (int64, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	var n json.Number
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&n); err != nil {
		return 0, err
	}
	return n.Int64()
}

func (s *Service) batchBaseKey(ctx context.Context, tx *sql.Tx, kind string, body map[string]any) (string, error) {
	symbol, _ := body["symbol"].(string)
	side, _ := body["side"].(string)
	position, _ := body["positionSide"].(string)
	if symbol == "" || side == "" || position == "" {
		return "", nil
	}
	if kind == "copy" {
		taskID, err := jsonNumber(body["copy_task_id"])
		if err != nil {
			return "", err
		}
		var orderKey string
		if err = tx.QueryRowContext(ctx, "SELECT order_key FROM copy_tasks WHERE id=?", taskID).Scan(&orderKey); err != nil {
			return "", err
		}
		base := orderKey
		if i := strings.Index(base, ":"); i >= 0 {
			base = base[:i]
		}
		return "copy|" + base, nil
	}
	// Group by symbol, direction and order creation time like the Python merge:
	// later updates of the same order keep the same orderTime.
	created, err := jsonNumber(body["orderTime"])
	if err != nil || created <= 0 {
		return "", nil
	}
	return groupKey(symbol, side, position, created), nil
}

func (s *Service) combineLeaderOrder(ctx context.Context, base string, event int64, bodyRaw string, body map[string]any, stamp int64) (string, []int64, error) {
	if stamp == 0 {
		return bodyRaw, nil, nil
	}
	rows, err := s.DB().QueryContext(ctx, `SELECT e.id,e.body,e.stamp FROM events e
		JOIN outbox o ON o.event=e.id AND o.status='pending'
		WHERE e.kind='order' AND e.id<? AND e.stamp>=? ORDER BY e.id`, event, stamp-15000)
	if err != nil {
		return bodyRaw, nil, err
	}
	var fills []domain.LeaderFill
	var absorb []int64
	for rows.Next() {
		var id, eventStamp int64
		var raw string
		if err = rows.Scan(&id, &raw, &eventStamp); err != nil {
			rows.Close()
			return bodyRaw, nil, err
		}
		var f domain.LeaderFill
		if err = json.Unmarshal([]byte(raw), &f); err != nil {
			rows.Close()
			return bodyRaw, nil, err
		}
		created, e := f.Created.Int64()
		measured := groupKey(f.Symbol, f.Side, f.PositionSide, created)
		if measured != base {
			continue
		}
		if e != nil {
			continue
		}
		fills = append(fills, f)
		absorb = append(absorb, id)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return bodyRaw, nil, err
	}
	rows.Close()
	var current domain.LeaderFill
	if err = json.Unmarshal([]byte(bodyRaw), &current); err != nil {
		return bodyRaw, nil, err
	}
	fills = append(fills, current)
	records, err := MergeLeaderRecords(fills)
	if err != nil {
		return bodyRaw, nil, err
	}
	for _, record := range records {
		if !sameDirection(record, body) {
			continue
		}
		raw, e := json.Marshal(record)
		if e != nil {
			return bodyRaw, nil, e
		}
		return string(raw), absorb, nil
	}
	return bodyRaw, nil, nil
}

func sameDirection(record, body map[string]any) bool {
	for _, field := range []string{"symbol", "side", "positionSide"} {
		if strings.EqualFold(strings.TrimSpace(asString(record[field])), strings.TrimSpace(asString(body[field]))) == false {
			return false
		}
	}
	return true
}

func asString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case nil:
		return ""
	default:
		raw, _ := json.Marshal(v)
		return strings.Trim(string(raw), `"`)
	}
}

func (s *Service) combineCopyTasks(base string, event int64, _ string, channel string) (string, []int64, error) {
	rows, err := s.DB().QueryContext(context.Background(), `SELECT e.id,e.body FROM events e JOIN outbox o ON o.event=e.id AND o.status='pending'
		WHERE e.kind='copy' AND e.id<? AND o.channel=? ORDER BY e.id`, event, channel)
	if err != nil {
		return "", nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		var raw string
		if err = rows.Scan(&id, &raw); err != nil {
			return "", nil, err
		}
		var previous map[string]any
		if err = json.Unmarshal([]byte(raw), &previous); err != nil {
			return "", nil, err
		}
		taskID, e := jsonNumber(previous["copy_task_id"])
		if e != nil {
			continue
		}
		var orderKey string
		if e = s.DB().QueryRowContext(context.Background(), "SELECT order_key FROM copy_tasks WHERE id=?", taskID).Scan(&orderKey); e != nil {
			if e == sql.ErrNoRows {
				continue
			}
			return "", nil, e
		}
		baseOf := orderKey
		if i := strings.Index(baseOf, ":"); i >= 0 {
			baseOf = baseOf[:i]
		}
		if "copy|"+baseOf == base {
			ids = append(ids, id)
		}
	}
	return "", ids, rows.Err()
}

func groupKey(symbol, side, position string, created int64) string {
	return strings.ToUpper(symbol) + "|" + strings.ToUpper(side) + "|" + strings.ToUpper(position) + "|" + itoa(created)
}

func itoa(v int64) string {
	return json.Number(strconv.FormatInt(v, 10)).String()
}
