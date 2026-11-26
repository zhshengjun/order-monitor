package exchange

import (
	"context"
	"encoding/json"
)

var portfolioIDPattern = `^[0-9]{1,30}$`

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
