package db

import (
	"context"
	"time"

	"github.com/stolostron/hub-of-hubs-data-types/bundle/spec"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
)

// SpecDB is the needed interface for the db transport bridge.
type SpecDB interface {
	// GetLastUpdateTimestamp returns the last update timestamp of a specific table.
	GetLastUpdateTimestamp(ctx context.Context, tableName string, tableHasResources bool) (*time.Time, error)
	// Stop stops db and releases resources (e.g. connection pool).
	Stop()

	ObjectsSpecDB
	ManagedClusterLabelsSpecDB
}

// ObjectsSpecDB is the interface needed by the spec transport bridge to sync objects tables.
type ObjectsSpecDB interface {
	// GetObjectsBundle returns a bundle of objects from a specific table.
	GetObjectsBundle(ctx context.Context, tableName string, createObjFunc bundle.CreateObjectFunction,
		intoBundle bundle.ObjectsBundle) (*time.Time, error)
}

// ManagedClusterLabelsSpecDB is the interface needed by the spec transport bridge to sync managed-cluster labels table.
type ManagedClusterLabelsSpecDB interface {
	// GetUpdatedManagedClusterLabelsBundles returns a map of leaf-hub -> ManagedClusterLabelsSpecBundle of objects
	// belonging to a leaf-hub that had at least one update since the given timestamp, from a specific table.
	GetUpdatedManagedClusterLabelsBundles(ctx context.Context, tableName string,
		timestamp *time.Time) (map[string]*spec.ManagedClusterLabelsSpecBundle, error)
}
