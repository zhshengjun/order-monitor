package monitor

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"order-monitor/internal/domain"
)

func TestNotificationTextTrimsDerivedPrice(t *testing.T) {
	body := map[string]any{
		"symbol": "ETHUSDT", "side": "BUY", "positionSide": "LONG",
		"executedQty": "2.227", "avgPrice": "1697.0700000000000000000000000000000000000000",
		"notional": "3779.37489", "totalPnl": "0", "baseAsset": "ETH", "quoteAsset": "USDT",
	}
	_, text := notificationText("Eth孙满仓", "order", 0, body)
	if strings.Contains(text, "1697.070000") {
		t.Fatalf("long price not trimmed:\n%s", text)
	}
	if !strings.Contains(text, "均价：1697.07 USDT") || !strings.Contains(text, "数量：2.227 ETH") {
		t.Fatalf("unexpected text:\n%s", text)
	}
}

func TestCombineBatchCollapsesSameOrderFills(t *testing.T) {
	s, err := OpenService(context.Background(), filepath.Join(t.TempDir(), "combine.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if _, err = s.DB().ExecContext(ctx, `INSERT INTO targets(id,name) VALUES('kol','KOL');
		INSERT OR REPLACE INTO settings(id,value) VALUES(1,'{"bark_enabled":true}')`); err != nil {
		t.Fatal(err)
	}
	base := map[string]any{"symbol": "ETHUSDT", "side": "BUY", "positionSide": "LONG", "orderTime": 1000, "orderUpdateTime": 1000, "baseAsset": "ETH", "quoteAsset": "USDT", "executedQty": "1", "avgPrice": "1000", "notional": "1000"}
	second := map[string]any{"symbol": "ETHUSDT", "side": "BUY", "positionSide": "LONG", "orderTime": 1000, "orderUpdateTime": 2000, "baseAsset": "ETH", "quoteAsset": "USDT", "executedQty": "2", "avgPrice": "1700", "notional": "3400"}
	insertEvent := func(body map[string]any) int64 {
		raw, _ := json.Marshal(body)
		res, e := s.DB().ExecContext(ctx, "INSERT INTO events(target,name,kind,body,stamp,historical) VALUES('kol','KOL','order',?,1000,0)", string(raw))
		if e != nil {
			t.Fatal(e)
		}
		id, _ := res.LastInsertId()
		if _, e = s.DB().ExecContext(ctx, "INSERT INTO outbox(event,channel) VALUES(?,'bark')", id); e != nil {
			t.Fatal(e)
		}
		return id
	}
	first := insertEvent(base)
	current := insertEvent(second)
	raw, _ := json.Marshal(second)
	combined, absorb, err := s.combineBatch(ctx, current, "bark", "order", string(raw), second)
	if err != nil {
		t.Fatal(err)
	}
	if len(absorb) != 1 || absorb[0] != first {
		t.Fatalf("absorbed %v", absorb)
	}
	var merged domain.LeaderFill
	if err = json.Unmarshal([]byte(combined), &merged); err != nil {
		t.Fatal(err)
	}
	if merged.Quantity.String() != "3" || merged.Notional.String() != "4400" {
		t.Fatalf("merged %+v", merged)
	}
	if price := compactNumber(merged.Price.String()); price != "1466.6666666667" && price != "1466.67" && !strings.HasPrefix(price, "1466.66") {
		t.Fatalf("merged price %s", price)
	}
}
