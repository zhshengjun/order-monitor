package copy

import (
	"context"

	"order-monitor/internal/store"
)

// Service owns the copy-trading pipeline. It embeds the SQLite store so the
// feature package can keep its transactional methods while sharing persistence.
type Service struct {
	*store.SQLiteStore
}

func New(core *store.SQLiteStore) *Service { return &Service{SQLiteStore: core} }

// OpenService opens and initializes a persistent copy service. It exists for
// tests and for callers that only need the copy feature package.
func OpenService(ctx context.Context, path string) (*Service, error) {
	core, err := store.OpenSQLiteStore(ctx, path)
	if err != nil {
		return nil, err
	}
	if err = core.InitializeMonitorTables(ctx); err != nil {
		core.Close()
		return nil, err
	}
	return New(core), nil
}

// CopyNotice writes a copy event and its notification jobs in one transaction.
func (s *Service) CopyNotice(ctx context.Context, target string, body map[string]any) error {
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = copyNotice(ctx, tx, target, body); err != nil {
		return err
	}
	return tx.Commit()
}
