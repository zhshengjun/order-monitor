package monitor

import (
	"context"

	"order-monitor/internal/copy"
)

// OpenService opens the full monitoring service from a database path.
func OpenService(ctx context.Context, path string) (*Service, error) {
	service, err := copy.OpenService(ctx, path)
	if err != nil {
		return nil, err
	}
	return New(service), nil
}
