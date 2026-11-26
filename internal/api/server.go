package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"order-monitor/internal/copy"
	"order-monitor/internal/exchange"
	"order-monitor/internal/monitor"
	"order-monitor/internal/store"
	"order-monitor/internal/web"
)

func Handler(core *store.SQLiteStore, service *monitor.Service, leader *exchange.LeaderClient, leaderErr error, token string, secure bool, history *monitor.HistoryWorker) http.Handler {
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write([]byte("order-monitor-session-v1"))
	session := hex.EncodeToString(mac.Sum(nil))
	mux := http.NewServeMux()
	reply := func(w http.ResponseWriter, status int, data any) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(data)
	}
	fail := func(w http.ResponseWriter, status int, message string) {
		reply(w, status, map[string]string{"error": message})
	}
	auth := func(r *http.Request) bool {
		cookie, err := r.Cookie("om_session")
		return err == nil && hmac.Equal([]byte(cookie.Value), []byte(session))
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { reply(w, 200, map[string]bool{"ok": true}) })
	for path, file := range map[string]string{"/{$}": "index.html", "/app.js": "app.js", "/style.css": "style.css"} {
		mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
			data, err := web.Assets.ReadFile("static/" + file)
			if err != nil {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", mime.TypeByExtension(filepath.Ext(file)))
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("Content-Security-Policy", "default-src 'self'; img-src 'self' https:; style-src 'self'; script-src 'self'; frame-ancestors 'none'; base-uri 'none'")
			w.Write(data)
		})
	}
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r) {
			fail(w, 401, "请先登录")
			return
		}
		state, err := service.Dashboard(r.Context())
		if err != nil {
			fail(w, 500, "服务器处理失败")
			return
		}
		reply(w, 200, state)
	})
	mux.HandleFunc("GET /api/search", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r) {
			fail(w, 401, "请先登录")
			return
		}
		if leaderErr != nil {
			fail(w, 500, "带单员接口配置无效")
			return
		}
		page := 1
		if raw := r.URL.Query().Get("page"); raw != "" {
			var err error
			page, err = strconv.Atoi(raw)
			if err != nil {
				fail(w, 400, "页码无效")
				return
			}
		}
		result, err := leader.Search(r.Context(), strings.TrimSpace(r.URL.Query().Get("q")), page)
		if err != nil {
			fail(w, 400, err.Error())
			return
		}
		reply(w, 200, result)
	})
	mux.HandleFunc("GET /api/leader-positions", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r) {
			fail(w, 401, "请先登录")
			return
		}
		result, err := service.LeaderPositions(r.Context(), r.URL.Query().Get("target"), history != nil && history.Active(r.URL.Query().Get("target")))
		if err != nil {
			var invalid exchange.InputError
			if errors.As(err, &invalid) {
				fail(w, 400, invalid.Error())
			} else {
				fail(w, 500, "服务器处理失败")
			}
			return
		}
		reply(w, 200, result)
	})
	mux.HandleFunc("GET /api/futures-snapshot", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r) {
			fail(w, 401, "请先登录")
			return
		}
		id, err := strconv.ParseInt(r.URL.Query().Get("account_id"), 10, 64)
		if err != nil {
			fail(w, 400, "账号参数错误")
			return
		}
		data, err := service.Snapshot(r.Context(), id, &http.Client{Timeout: 15 * time.Second})
		if err != nil {
			var invalid exchange.InputError
			if errors.As(err, &invalid) {
				fail(w, 400, invalid.Error())
			} else {
				fail(w, 400, "账户查询失败")
			}
			return
		}
		reply(w, 200, data)
	})
	mux.HandleFunc("GET /api/leader-mark-prices", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r) {
			fail(w, 401, "请先登录")
			return
		}
		data, _ := exchange.MarkPrices(r.Context(), &http.Client{Timeout: 15 * time.Second})
		reply(w, 200, data)
	})
	mux.HandleFunc("GET /api/futures-network", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r) {
			fail(w, 401, "请先登录")
			return
		}
		reply(w, 200, exchange.FuturesNetwork(r.Context(), &http.Client{Timeout: 15 * time.Second}))
	})
	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r) {
			fail(w, 401, "请先登录")
			return
		}
		q := r.URL.Query()
		page, limit := 1, 50
		var err error
		if q.Has("page") {
			page, err = strconv.Atoi(q.Get("page"))
			if err != nil {
				fail(w, 400, "页码无效")
				return
			}
		}
		if q.Has("limit") {
			limit, err = strconv.Atoi(q.Get("limit"))
			if err != nil {
				fail(w, 400, "每页数量无效")
				return
			}
		}
		result, err := service.Events(r.Context(), q.Get("target"), q.Get("kind"), page, limit)
		if err != nil {
			fail(w, 500, "服务器处理失败")
			return
		}
		reply(w, 200, result)
	})
	mux.HandleFunc("GET /api/copy-task-pnl", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r) {
			fail(w, 401, "请先登录")
			return
		}
		q := r.URL.Query()
		account, err := strconv.ParseInt(q.Get("account_id"), 10, 64)
		if err != nil {
			fail(w, 400, "账号参数错误")
			return
		}
		task, err := strconv.ParseInt(q.Get("task_id"), 10, 64)
		if err != nil {
			fail(w, 400, "跟单记录参数错误")
			return
		}
		result, err := service.CopyPnL(r.Context(), account, task, &http.Client{Timeout: 15 * time.Second})
		if err != nil {
			var invalid exchange.InputError
			if errors.As(err, &invalid) {
				fail(w, 400, invalid.Error())
			} else {
				fail(w, 500, "服务器处理失败")
			}
			return
		}
		reply(w, 200, result)
	})
	mux.HandleFunc("GET /api/copy-tasks", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r) {
			fail(w, 401, "请先登录")
			return
		}
		q := r.URL.Query()
		account, err := strconv.ParseInt(q.Get("account_id"), 10, 64)
		if err != nil {
			fail(w, 400, "账号参数错误")
			return
		}
		page, limit := 1, 50
		if q.Has("page") {
			page, err = strconv.Atoi(q.Get("page"))
			if err != nil {
				fail(w, 400, "页码无效")
				return
			}
		}
		if q.Has("limit") {
			limit, err = strconv.Atoi(q.Get("limit"))
			if err != nil {
				fail(w, 400, "每页数量无效")
				return
			}
		}
		result, err := service.CopyRecords(r.Context(), account, page, limit)
		if err != nil {
			var invalid exchange.InputError
			if errors.As(err, &invalid) {
				fail(w, 400, invalid.Error())
			} else {
				fail(w, 500, "服务器处理失败")
			}
			return
		}
		reply(w, 200, result)
	})
	for _, path := range []string{"/api/login", "/api/logout", "/api/toggle", "/api/copy-binding", "/api/copy-multiplier", "/api/copy-live", "/api/virtual-session", "/api/targets", "/api/display-order", "/api/futures-account", "/api/settings", "/api/leader-history-refresh", "/api/delete-target", "/api/delete-account", "/api/test-notification", "/api/resolve-copy-task", "/api/copy-position"} {
		mux.HandleFunc("POST "+path, func(w http.ResponseWriter, r *http.Request) {
			if origin := r.Header.Get("Origin"); origin != "" {
				u, err := url.Parse(origin)
				if err != nil || u.Host != r.Host {
					fail(w, 403, "跨站请求已拒绝")
					return
				}
			}
			contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil || contentType != "application/json" {
				fail(w, 415, "需要 application/json")
				return
			}
			raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16384))
			var body map[string]any
			if err != nil || json.Unmarshal(raw, &body) != nil || body == nil {
				fail(w, 400, "请求体必须为对象")
				return
			}
			cookie := &http.Cookie{Name: "om_session", Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: secure}
			if path == "/api/login" {
				input, _ := body["token"].(string)
				if !hmac.Equal([]byte(input), []byte(token)) {
					time.Sleep(500 * time.Millisecond)
					fail(w, 401, "访问口令错误")
					return
				}
				cookie.Value = session
			} else {
				if !auth(r) {
					fail(w, 401, "请先登录")
					return
				}
				if path != "/api/logout" {
					var input store.ControlInput
					if err := json.Unmarshal(raw, &input); err != nil {
						fail(w, 400, "参数错误")
						return
					}
					var operationErr error
					switch path {
					case "/api/resolve-copy-task":
						var request struct {
							ID int64 `json:"id"`
						}
						if json.Unmarshal(raw, &request) != nil || request.ID <= 0 {
							fail(w, 400, "跟单记录参数错误")
							return
						}
						result, err := service.ResolveSubmittedCopy(r.Context(), request.ID, copy.ClientFactory(&http.Client{Timeout: 15 * time.Second}))
						if err != nil {
							var invalid store.InputError
							if errors.As(err, &invalid) {
								fail(w, 400, invalid.Error())
							} else {
								fail(w, 500, "订单核对失败")
							}
							return
						}
						reply(w, 200, map[string]any{"ok": true, "result": result})
						return
					case "/api/copy-position":
						var request copy.PositionAdjustment
						if json.Unmarshal(raw, &request) != nil {
							fail(w, 400, "持仓修正参数错误")
							return
						}
						if err := service.AdjustCopyPosition(r.Context(), request); err != nil {
							var invalid store.InputError
							if errors.As(err, &invalid) {
								fail(w, 400, invalid.Error())
							} else {
								fail(w, 500, "服务器处理失败")
							}
							return
						}
						reply(w, 200, map[string]bool{"ok": true})
						return
					case "/api/test-notification":
						operationErr = service.TestNotification(r.Context(), &http.Client{Timeout: 15 * time.Second}, body)
					case "/api/delete-target", "/api/delete-account":
						if history == nil {
							fail(w, 500, "后台服务不可用")
							return
						}
						isTarget := path == "/api/delete-target"
						var request struct {
							ID json.RawMessage `json:"id"`
						}
						if json.Unmarshal(raw, &request) != nil {
							fail(w, 400, "参数错误")
							return
						}
						var id any
						if isTarget {
							var value string
							if json.Unmarshal(request.ID, &value) != nil {
								fail(w, 400, "带单员参数错误")
								return
							}
							id = value
						} else {
							var value int64
							if json.Unmarshal(request.ID, &value) != nil {
								fail(w, 400, "账号参数错误")
								return
							}
							id = value
						}
						operationErr = history.Delete(r.Context(), isTarget, id, &http.Client{Timeout: 15 * time.Second})
					case "/api/leader-history-refresh":
						var req struct {
							Target string `json:"target"`
						}
						if json.Unmarshal(raw, &req) != nil || history == nil {
							fail(w, 400, "参数错误")
							return
						}
						operationErr = history.Refresh(r.Context(), req.Target)
					case "/api/settings":
						operationErr = service.SaveSettings(r.Context(), body)
					case "/api/futures-account":
						var account store.AccountInput
						if json.Unmarshal(raw, &account) != nil {
							fail(w, 400, "账号参数错误")
							return
						}
						operationErr = service.SaveAccount(r.Context(), account)
					case "/api/targets":
						if leaderErr != nil {
							fail(w, 500, "带单员接口配置无效")
							return
						}
						operationErr = service.AddTarget(r.Context(), leader, input.ID, time.Now())
					case "/api/display-order":
						var order struct {
							Kind     string
							ID       string
							Relative string `json:"relative_id"`
							Side     string
						}
						if json.Unmarshal(raw, &order) != nil {
							fail(w, 400, "排序参数无效")
							return
						}
						operationErr = service.Reorder(r.Context(), order.Kind, order.ID, order.Relative, order.Side)
					default:
						operationErr = service.ApplyControl(r.Context(), path, input, time.Now())
					}
					if err := operationErr; err != nil {
						var invalid exchange.InputError
						if errors.As(err, &invalid) {
							fail(w, 400, invalid.Error())
						} else {
							fail(w, 500, "服务器处理失败")
						}
						return
					}
					reply(w, 200, map[string]bool{"ok": true})
					return
				}
				cookie.MaxAge = -1
			}
			http.SetCookie(w, cookie)
			reply(w, 200, map[string]bool{"ok": true})
		})
	}
	return mux
}
