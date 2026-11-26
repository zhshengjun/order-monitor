package engine

import (
	"context"
	"database/sql"
	"errors"
)

// ExecuteCopy processes an account-specific durable task. A restart after
// submission reconciles the original order even if the position has changed.
func (s *SQLiteStore) ExecuteCopy(ctx context.Context, exchange OrderExchange, taskKey string, input PlanInput) (Submission, error) {
	previous, err := s.Submission(ctx, taskKey)
	if err == nil {
		return s.Execute(ctx, exchange, taskKey, previous.Order)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Submission{}, err
	}
	change, err := TransitionLeaderPosition(input.LeaderBefore, input.Key.Side, input.Key.PositionSide, input.Signal.Quantity)
	if err != nil {
		return Submission{}, err
	}
	input.Action = change.Action
	order, err := PlanOrder(input)
	if err != nil {
		return Submission{}, err
	}
	return s.Execute(ctx, exchange, taskKey, order)
}
