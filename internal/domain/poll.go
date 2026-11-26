package domain

import (
	"context"
	"time"
)

// Poll calls next serially. Successful polls have no fixed interval. Error
// backoff is supplied by the caller so rate-limit responses can be respected.
func Poll(ctx context.Context, next func(context.Context) error, backoff func(error) time.Duration) error {
	for ctx.Err() == nil {
		err := next(ctx)
		if err == nil {
			continue
		}
		delay := backoff(err)
		if delay <= 0 {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}
