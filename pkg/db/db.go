package db

import (
	"context"
	"time"

	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
)

// SpecDB is the needed interface for the db transport bridge.
type SpecDB interface {
	// GetBundle returns a bundle of objects from a specific table.
	GetBundle(ctx context.Context, tableName string, createObjFunc bundle.CreateObjectFunction,
		intoBundle bundle.Bundle) (*time.Time, error)
	// GetLastUpdateTimestamp returns the last update timestamp of a specific table.
	GetLastUpdateTimestamp(ctx context.Context, tableName string) (*time.Time, error)
	// Stop stops db and releases resources (e.g. connection pool).
	Stop()
}
