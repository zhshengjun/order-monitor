package app

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"order-monitor/internal/api"
	"order-monitor/internal/copy"
	"order-monitor/internal/exchange"
	"order-monitor/internal/monitor"
	"order-monitor/internal/store"
)

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func Run() error {
	token := os.Getenv("ADMIN_TOKEN")
	if len(token) < 16 {
		return errors.New("请设置至少 16 位的 ADMIN_TOKEN 环境变量")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	path := env("DATABASE_PATH", "data/monitor.db")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if err = file.Chmod(0600); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	core, err := store.OpenSQLiteStore(ctx, path)
	if err != nil {
		return err
	}
	defer core.Close()
	if err = core.InitializeMonitorTables(ctx); err != nil {
		return err
	}
	leader, err := exchange.NewLeaderClient(&http.Client{Timeout: 20 * time.Second}, strings.TrimRight(env("BINANCE_BASE_URL", "https://www.binance.com"), "/"))
	if err != nil {
		return err
	}
	service := monitor.New(copy.New(core))
	history := monitor.NewHistoryWorker(service, leader)
	server := &http.Server{Addr: net.JoinHostPort(env("HOST", "127.0.0.1"), env("PORT", "8080")), Handler: api.Handler(core, service, leader, nil, token, os.Getenv("COOKIE_SECURE") == "1", history), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := core.MigrateLegacyHistory(ctx); err != nil {
		return err
	}
	if err := core.ImportFillBaselines(ctx); err != nil {
		return err
	}
	backgroundClient := &http.Client{Timeout: 15 * time.Second}
	watchDone := make(chan struct{})
	go func() { defer close(watchDone); service.RunCloseWatch(ctx, backgroundClient) }()
	defer func() { cancel(); <-watchDone }()
	riskDone := make(chan struct{})
	go func() { defer close(riskDone); service.RunRiskMonitor(ctx, backgroundClient) }()
	defer func() { cancel(); <-riskDone }()
	notificationsDone := make(chan struct{})
	go func() { defer close(notificationsDone); service.RunNotifications(ctx, backgroundClient) }()
	defer func() { cancel(); <-notificationsDone }()
	marginDone := make(chan struct{})
	go func() { defer close(marginDone); service.RunMarginMonitor(ctx, backgroundClient) }()
	defer func() { cancel(); <-marginDone }()
	transferDone := make(chan struct{})
	go func() { defer close(transferDone); service.RunTransfers(ctx, leader) }()
	defer func() { cancel(); <-transferDone }()
	historyDone := make(chan struct{})
	go func() { defer close(historyDone); history.Run(ctx) }()
	defer func() { cancel(); <-historyDone }()
	leaderDone := make(chan struct{})
	go func() { defer close(leaderDone); service.RunLeaderMonitor(ctx, leader) }()
	defer func() { cancel(); <-leaderDone }()
	recoveryDone := make(chan struct{})
	go func() {
		defer close(recoveryDone)
		service.RunCopyWorker(ctx, &http.Client{Timeout: exchange.FuturesRequestTimeout}, leader)
	}()
	defer func() { cancel(); <-recoveryDone }()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(ctx)
	}
}
