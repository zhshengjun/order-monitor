package monitor

import (
	"order-monitor/internal/copy"
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
	LeaderPage      = domain.LeaderPage
	LeaderDetail    = domain.LeaderDetail
	OrderResult     = exchange.OrderResult
	ExchangeError   = exchange.ExchangeError
	FuturesClient   = exchange.FuturesClient
	LeaderClient    = exchange.LeaderClient
	FuturesBalance  = exchange.FuturesBalance
	FuturesPosition = exchange.FuturesPosition
	InputError      = domain.InputError
)

var (
	decimal                  = domain.Decimal
	signedDecimal            = domain.SignedDecimal
	positiveDecimal          = domain.PositiveDecimal
	decimalString            = domain.DecimalString
	decimalText              = domain.DecimalText
	noticeNumber             = domain.NoticeNumber
	Direction                = domain.Direction
	RecordIdentity           = domain.RecordIdentity
	HistoryIdentity          = domain.HistoryIdentity
	ReplayPositions          = domain.ReplayPositions
	NewComparator            = domain.NewComparator
	PlanOrder                = domain.PlanOrder
	TransitionLeaderPosition = domain.TransitionLeaderPosition
	DashboardRows            = store.DashboardRows
	QueueEvent               = store.QueueEvent
	MergeLeaderRecords       = store.MergeLeaderRecords
	DefaultSettings          = store.DefaultSettings
	PositionAmount           = exchange.PositionAmount
	defaultSettings          = store.DefaultSettings
	DecodeRecord             = store.DecodeRecord
	_                        = copy.New
)
