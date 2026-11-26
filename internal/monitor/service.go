package monitor

import (
	"order-monitor/internal/copy"
)

// Service owns leader polling, transfers, notifications and risk monitoring.
// It embeds the copy service so shared persistence and helpers are promoted.
type Service struct {
	*copy.Service
}

func New(c *copy.Service) *Service { return &Service{Service: c} }
