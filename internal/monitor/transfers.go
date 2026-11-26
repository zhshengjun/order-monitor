package monitor

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"order-monitor/internal/exchange"
	"time"
)

type transferPage struct {
	List  []json.RawMessage `json:"list"`
	Total json.Number       `json:"total"`
}
type transferRecord struct {
	Time   json.Number `json:"time"`
	Coin   string      `json:"coin"`
	Amount any         `json:"amount"`
	From   any         `json:"from"`
	To     any         `json:"to"`
	Type   any         `json:"transType"`
}

func transfers(c *exchange.LeaderClient, ctx context.Context, target string, page, size int) (transferPage, error) {
	body, err := json.Marshal(map[string]any{"portfolioId": target, "pageNumber": page, "pageSize": size})
	if err != nil {
		return transferPage{}, err
	}
	var result transferPage
	err = c.Request(ctx, "lead-portfolio/transfer-history", body, &result)
	if err == nil && (result.List == nil || result.Total == "") {
		return result, errors.New("历史响应结构异常，未推进同步位置")
	}
	return result, err
}
func (s *Service) pollTransfers(ctx context.Context, c *exchange.LeaderClient, target, name string, size int) error {
	var watermark sql.NullInt64
	if err := s.DB().QueryRowContext(ctx, "SELECT MAX(stamp) FROM records WHERE target=? AND kind='transfer'", target).Scan(&watermark); err != nil {
		return err
	}
	var rows []json.RawMessage
	crossed, complete := false, false
	for page := 1; page <= 50; page++ {
		batch, err := transfers(c, ctx, target, page, size)
		if err != nil {
			return err
		}
		rows = append(rows, batch.List...)
		if len(batch.List) == 0 || !watermark.Valid {
			complete = true
			break
		}
		if crossed {
			complete = true
			break
		}
		for _, raw := range batch.List {
			var row transferRecord
			if err = json.Unmarshal(raw, &row); err != nil {
				return err
			}
			stamp, err := row.Time.Int64()
			if err != nil {
				return err
			}
			if stamp < watermark.Int64 {
				crossed = true
			}
		}
	}
	if !complete {
		return errors.New("新增划转记录过多，未推进同步位置")
	}
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var enabled bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM targets WHERE id=? AND enabled=1)", target).Scan(&enabled); err != nil {
		return err
	}
	if !enabled {
		return tx.Commit()
	}
	var baseline int64
	err = tx.QueryRowContext(ctx, "SELECT stamp FROM baseline_watermarks WHERE target=? AND kind='transfer'", target).Scan(&baseline)
	first := errors.Is(err, sql.ErrNoRows)
	if err != nil && !first {
		return err
	}
	type entry struct {
		raw    json.RawMessage
		record transferRecord
		stamp  int64
	}
	parsed := make([]entry, 0, len(rows))
	var maxStamp int64
	for _, raw := range rows {
		var row transferRecord
		if err = json.Unmarshal(raw, &row); err != nil {
			return err
		}
		stamp, err := row.Time.Int64()
		if err != nil || row.Coin == "" || row.Amount == nil || row.Type == nil {
			return errors.New("历史记录字段缺失，未推进同步位置")
		}
		parsed = append(parsed, entry{raw, row, stamp})
		if stamp > maxStamp {
			maxStamp = stamp
		}
	}
	for i := len(parsed) - 1; i >= 0; i-- {
		row := parsed[i]
		rawKey, err := json.Marshal([]any{row.stamp, row.record.Coin, row.record.Amount, row.record.From, row.record.To, row.record.Type})
		if err != nil {
			return err
		}
		hash := sha256.Sum256(rawKey)
		key := hex.EncodeToString(hash[:])
		var previous string
		err = tx.QueryRowContext(ctx, "SELECT body FROM records WHERE target=? AND kind='transfer' AND key=?", target, key).Scan(&previous)
		exists := err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if exists && json.Valid([]byte(previous)) && previous == string(row.raw) {
			continue
		}
		if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO records VALUES(?,'transfer',?,?,?)", target, key, string(row.raw), row.stamp); err != nil {
			return err
		}
		if exists {
			continue
		}
		historical := first || row.stamp <= baseline
		res, err := tx.ExecContext(ctx, "INSERT INTO events(target,name,kind,body,stamp,historical) VALUES(?,?,'transfer',?,?,?)", target, name, string(row.raw), row.stamp, historical)
		if err != nil {
			return err
		}
		if !historical {
			event, err := res.LastInsertId()
			if err != nil {
				return err
			}
			if err = queueEvent(ctx, tx, event); err != nil {
				return err
			}
		}
	}
	if first {
		if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO baseline_watermarks VALUES(?,'transfer',?)", target, maxStamp); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func queueEvent(ctx context.Context, tx *sql.Tx, event int64) error {
	var raw string
	err := tx.QueryRowContext(ctx, "SELECT value FROM settings WHERE id=1").Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var settings map[string]any
	if err = json.Unmarshal([]byte(raw), &settings); err != nil {
		return err
	}
	for _, channel := range []string{"telegram", "bark"} {
		if settings[channel+"_enabled"] == true {
			if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO outbox(event,channel) VALUES(?,?)", event, channel); err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *Service) RunTransfers(ctx context.Context, c *exchange.LeaderClient) {
	next := map[string]time.Time{}
	for ctx.Err() == nil {
		rows, err := s.DB().QueryContext(ctx, "SELECT t.id,t.name,COALESCE(json_extract(s.value,'$.page_size'),20) FROM targets t LEFT JOIN settings s ON s.id=1 WHERE t.enabled=1")
		type target struct {
			id, name string
			size     int
		}
		var targets []target
		if err == nil {
			for rows.Next() {
				var t target
				if err = rows.Scan(&t.id, &t.name, &t.size); err != nil {
					break
				}
				targets = append(targets, t)
			}
			if err == nil {
				err = rows.Err()
			}
			rows.Close()
		}
		if err != nil {
			log.Printf("查询划转监听对象失败：%v", err)
		}
		for _, t := range targets {
			if time.Now().Before(next[t.id]) {
				continue
			}
			next[t.id] = time.Now().Add(time.Minute)
			if err = s.pollTransfers(ctx, c, t.id, t.name, t.size); err != nil && ctx.Err() == nil {
				_, _ = s.DB().ExecContext(ctx, "UPDATE targets SET error=? WHERE id=?", err.Error(), t.id)
				log.Printf("划转同步失败 target=%s: %v", t.id, err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
