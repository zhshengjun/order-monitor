package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"order-monitor/internal/copy"
	"order-monitor/internal/monitor"
	"order-monitor/internal/store"
)

func TestDashboardRequiresLoginAndUsesOriginalContract(t *testing.T) {
	ctx := context.Background()
	core, err := store.OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "monitor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	if err := core.InitializeMonitorTables(ctx); err != nil {
		t.Fatal(err)
	}
	feature := monitor.New(copy.New(core))
	handler := Handler(core, feature, nil, nil, "test-admin-password", false, nil)
	get := func(cookie *http.Cookie) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/api/state", nil)
		if cookie != nil {
			req.AddCookie(cookie)
		}
		out := httptest.NewRecorder()
		handler.ServeHTTP(out, req)
		return out
	}
	if got := get(nil).Code; got != 401 {
		t.Fatalf("unauthenticated status %d", got)
	}
	metrics := httptest.NewRequest("GET", "/metrics", nil)
	metricsOut := httptest.NewRecorder()
	handler.ServeHTTP(metricsOut, metrics)
	if metricsOut.Code != 200 || !strings.Contains(metricsOut.Body.String(), "order_monitor_up 1") {
		t.Fatalf("metrics: %d %q", metricsOut.Code, metricsOut.Body.String())
	}
	login := httptest.NewRequest("POST", "/api/login", strings.NewReader(`{"token":"test-admin-password"}`))
	login.Header.Set("Content-Type", "application/json")
	out := httptest.NewRecorder()
	handler.ServeHTTP(out, login)
	if out.Code != 200 {
		t.Fatal(out.Body.String())
	}
	state := get(out.Result().Cookies()[0])
	if state.Code != 200 {
		t.Fatal(state.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(state.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"targets", "futures_accounts", "copy_bindings", "virtual_sessions", "virtual_positions", "copied_positions", "virtual_events", "copy_tasks", "copy_live", "unresolved_orders", "unresolved_manual_closes", "notification_errors"} {
		if _, ok := body[key].([]any); !ok {
			t.Errorf("missing array %s", key)
		}
	}
	settings := body["settings"].(map[string]any)
	for _, key := range []string{"telegram_token", "bark_key"} {
		if _, ok := settings[key]; ok {
			t.Errorf("secret included: %s", key)
		}
	}
}
