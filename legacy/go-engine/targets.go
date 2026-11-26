package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"time"
)

var portfolioID = regexp.MustCompile(`^[0-9]{1,30}$`)

func (c *LeaderClient) Search(ctx context.Context, nickname string, page int) (json.RawMessage, error) {
	if nickname == "" || len([]rune(nickname)) > 80 {
		return nil, InputError("请输入 1～80 字的昵称")
	}
	body, err := json.Marshal(map[string]any{"favoriteOnly": false, "dataType": "PNL", "timeRange": "30D", "apiKeyOnly": "false", "pageNumber": max(1, min(100, page)), "portfolioType": "ALL", "hideFull": "false", "nickname": nickname, "pageSize": 20})
	if err != nil {
		return nil, err
	}
	var data json.RawMessage
	err = c.request(ctx, "home-page/query-list", body, &data)
	return data, err
}

func (s *SQLiteStore) AddTarget(ctx context.Context, c *LeaderClient, id string, now time.Time) error {
	if !portfolioID.MatchString(id) {
		return InputError("组合 ID 格式错误")
	}
	var data json.RawMessage
	if err := c.request(ctx, "lead-portfolio/detail?"+url.Values{"portfolioId": {id}}.Encode(), nil, &data); err != nil {
		return err
	}
	var detail LeaderDetail
	if err := json.Unmarshal(data, &detail); err != nil {
		return err
	}
	if detail.ID != id || detail.MarginBalance == "" {
		return InputError("详情缺少组合 ID 或余额")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO targets(id,name,detail) VALUES(?,?,?)", id, detail.Name, string(data)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO monitor_start VALUES(?,?)", id, now.UnixMilli()); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) Reorder(ctx context.Context, kind, item, relative, side string) error {
	if (kind != "targets" && kind != "accounts") || (side != "before" && side != "after") {
		return InputError("排序参数无效")
	}
	if !portfolioID.MatchString(item) || !portfolioID.MatchString(relative) {
		return InputError("排序对象无效")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	query := dashboardQueries["targets"]
	if kind == "accounts" {
		query = dashboardQueries["futures_accounts"]
	}
	rows, err := dashboardRows(ctx, tx, query)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, fmt.Sprint(row["id"]))
	}
	i := slices.Index(ids, item)
	if i < 0 || !slices.Contains(ids, relative) {
		return InputError("排序对象不存在，请刷新后重试")
	}
	if item == relative {
		return tx.Commit()
	}
	ids = slices.Delete(ids, i, i+1)
	at := slices.Index(ids, relative)
	if side == "after" {
		at++
	}
	ids = slices.Insert(ids, at, item)
	if _, err = tx.ExecContext(ctx, "DELETE FROM display_order WHERE kind=?", kind); err != nil {
		return err
	}
	for i, id := range ids {
		if _, err = tx.ExecContext(ctx, "INSERT INTO display_order VALUES(?,?,?)", kind, id, i); err != nil {
			return err
		}
	}
	return tx.Commit()
}
