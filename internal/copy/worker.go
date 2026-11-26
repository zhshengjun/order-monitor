package copy

import (
	"context"
	"log"
	"net/http"
	"order-monitor/internal/exchange"
	"time"
)

// RunOrderRecovery resumes checking submitted tasks from the shared account
// ledger. New ticks are needed only when there is no due task or the query fails.
func (s *Service) RunCopyWorker(ctx context.Context, client *http.Client, leader *exchange.LeaderClient) {
	lookup := func(key, secret string) (OrderLookup, error) {
		return exchange.NewFuturesClient(client, "https://fapi.binance.com", key, secret)
	}
	for ctx.Err() == nil {
		didWork, err := s.ReconcileNext(ctx, time.Now(), lookup)
		if !didWork && err == nil {
			didWork, err = s.executeNextPending(ctx, client, leader)
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("跟单订单核对失败：%v", err)
		}
		if didWork && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
