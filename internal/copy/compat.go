package copy

import (
	"context"
	"database/sql"

	"order-monitor/internal/domain"
	"order-monitor/internal/exchange"
	"order-monitor/internal/store"
)

type (
	OrderKey        = domain.OrderKey
	CumulativeFill  = domain.CumulativeFill
	Observation     = domain.Observation
	Comparison      = domain.Comparison
	Comparator      = domain.Comparator
	LeaderFill      = domain.LeaderFill
	PlanInput       = domain.PlanInput
	MarketOrder     = domain.MarketOrder
	Market          = domain.Market
	Signal          = domain.Signal
	PositionChange  = domain.PositionChange
	InputError      = domain.InputError
	OrderResult     = exchange.OrderResult
	ExchangeError   = exchange.ExchangeError
	FuturesClient   = exchange.FuturesClient
	LeaderClient    = exchange.LeaderClient
	FuturesBalance  = exchange.FuturesBalance
	FuturesPosition = exchange.FuturesPosition
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
	TransitionLeaderPosition = domain.TransitionLeaderPosition
	PlanOrder                = domain.PlanOrder
	NewComparator            = domain.NewComparator
	ReplayPositions          = domain.ReplayPositions
	PositionAmount           = exchange.PositionAmount
	NewFuturesClient         = exchange.NewFuturesClient
	ErrFillRegression        = domain.ErrFillRegression
	DashboardRows            = store.DashboardRows
	QueueEvent               = store.QueueEvent
	ExecutionNotional        = exchange.ExecutionNotional
)

var _ = context.Background
var _ = sql.ErrNoRows
