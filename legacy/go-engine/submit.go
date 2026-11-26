package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

type OrderExchange interface {
	Submit(context.Context, MarketOrder, string) (OrderResult, error)
	Order(context.Context, string, string) (OrderResult, error)
}

type Submission struct {
	TaskKey  string
	ClientID string
	Order    MarketOrder
	State    string
	Result   OrderResult
}

// Execute persists the original client ID before issuing an order. Once any
// submission has started, future invocations only query the original order.
func (s *SQLiteStore) Execute(ctx context.Context, exchange OrderExchange, taskKey string, order MarketOrder) (Submission, error) {
	if taskKey == "" || exchange == nil {
		return Submission{}, errors.New("task key and order exchange required")
	}
	encoded, err := json.Marshal(order)
	if err != nil {
		return Submission{}, err
	}
	digest := sha256.Sum256([]byte(taskKey))
	clientID := "cp_" + hex.EncodeToString(digest[:15])
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO go_submissions(task_key,client_id,order_body,state)
		VALUES(?,?,?,'submitted') ON CONFLICT(task_key) DO NOTHING`,
		taskKey, clientID, string(encoded))
	if err != nil {
		return Submission{}, err
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return Submission{}, err
	}
	submission := Submission{TaskKey: taskKey, ClientID: clientID, Order: order, State: "submitted"}
	var result OrderResult
	if inserted == 1 {
		result, err = exchange.Submit(ctx, order, clientID)
		var rejection ExchangeError
		if errors.As(err, &rejection) && rejection.DefinitelyRejected() {
			_, markErr := s.db.ExecContext(ctx, "UPDATE go_submissions SET state='rejected' WHERE task_key=? AND state='submitted'", taskKey)
			if markErr != nil {
				return submission, errors.Join(err, markErr)
			}
			submission.State = "rejected"
			return submission, err
		}
	} else {
		submission, err = s.Submission(ctx, taskKey)
		if err != nil || submission.State != "submitted" {
			return submission, err
		}
		result, err = exchange.Order(ctx, submission.Order.Symbol, submission.ClientID)
	}
	if err != nil {
		return submission, err
	}

	if result.ClientID != submission.ClientID || result.Symbol != submission.Order.Symbol ||
		result.Side != submission.Order.Side || result.PositionSide != submission.Order.PositionSide ||
		result.OrderID == "" {
		return submission, errors.New("original order response does not match submitted order")
	}
	switch result.Status {
	case "FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH":
	default:
		return submission, nil
	}
	executed, err := decimal(result.Executed)
	if err != nil {
		return submission, fmt.Errorf("invalid confirmed execution: %w", err)
	}
	requested, err := positiveDecimal(submission.Order.Quantity, "requested quantity")
	if err != nil {
		return submission, err
	}
	if executed.Cmp(requested) > 0 || (result.Status == "FILLED" && executed.Sign() == 0) {
		return submission, errors.New("confirmed quantity conflicts with original order")
	}
	body, err := json.Marshal(result)
	if err != nil {
		return submission, err
	}
	if _, err := s.db.ExecContext(ctx,
		"UPDATE go_submissions SET state='finished',result=? WHERE task_key=? AND state='submitted'",
		string(body), taskKey); err != nil {
		return submission, err
	}
	return s.Submission(ctx, taskKey)
}

func (s *SQLiteStore) Submission(ctx context.Context, taskKey string) (Submission, error) {
	submission := Submission{TaskKey: taskKey}
	var body string
	var result sql.NullString
	err := s.db.QueryRowContext(ctx, "SELECT client_id,order_body,state,result FROM go_submissions WHERE task_key=?", taskKey).
		Scan(&submission.ClientID, &body, &submission.State, &result)
	if err != nil {
		return Submission{}, err
	}
	if err := json.Unmarshal([]byte(body), &submission.Order); err != nil {
		return Submission{}, err
	}
	if result.Valid {
		if err := json.Unmarshal([]byte(result.String), &submission.Result); err != nil {
			return Submission{}, err
		}
	}
	return submission, nil
}
