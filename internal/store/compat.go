package store

import (
	"context"
	"database/sql"
	"order-monitor/internal/domain"
	"order-monitor/internal/exchange"
)

type (
	OrderKey        = domain.OrderKey
	CumulativeFill  = domain.CumulativeFill
	Observation     = domain.Observation
	Comparison      = domain.Comparison
	Comparator      = domain.Comparator
	LeaderFill      = domain.LeaderFill
	LeaderPage      = domain.LeaderPage
	LeaderDetail    = domain.LeaderDetail
	LeaderClient    = exchange.LeaderClient
	FuturesClient   = exchange.FuturesClient
	FuturesPosition = exchange.FuturesPosition
	FuturesBalance  = exchange.FuturesBalance
	SymbolRules     = exchange.SymbolRules
	Market          = domain.Market
	MarketOrder     = domain.MarketOrder
	PositionChange  = domain.PositionChange
	PlanInput       = domain.PlanInput
	Signal          = domain.Signal
	InputError      = domain.InputError
	OrderResult     = exchange.OrderResult
	ExchangeError   = exchange.ExchangeError
)

var (
	decimal                  = domain.Decimal
	signedDecimal            = domain.SignedDecimal
	positiveDecimal          = domain.PositiveDecimal
	decimalString            = domain.DecimalString
	decimalText              = domain.DecimalText
	noticeNumber             = domain.NoticeNumber
	direction                = domain.Direction
	recordIdentity           = domain.RecordIdentity
	historyIdentity          = domain.HistoryIdentity
	ReplayPositions          = domain.ReplayPositions
	NewComparator            = domain.NewComparator
	TransitionLeaderPosition = domain.TransitionLeaderPosition
	PlanOrder                = domain.PlanOrder
)

func QueueEvent(ctx context.Context, tx *sql.Tx, event int64) error {
	return queueEvent(ctx, tx, event)
}

func DashboardRows(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]map[string]any, error) {
	return dashboardRows(ctx, tx, query, args...)
}

func DefaultSettings() map[string]any { return defaultSettings() }

func ValidPortfolioID(id string) bool { return portfolioID.MatchString(id) }
