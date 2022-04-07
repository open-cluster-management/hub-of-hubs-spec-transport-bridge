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
	GetLastUpdateTimestamp(ctx context.Context, tableName string, filterLocalResources bool) (*time.Time, error)

	ObjectsSpecDB
	ManagedClusterLabelsSpecDB
	ManagedClusterSetsSpecDB
}

// ObjectsSpecDB is the interface needed by the spec transport bridge to sync objects tables.
type ObjectsSpecDB interface {
	// GetObjectsBundle returns a bundle of objects from a specific table.
	GetObjectsBundle(ctx context.Context, tableName string, createObjFunc bundle.CreateObjectFunction,
		intoBundle bundle.ObjectsBundle) (*time.Time, error)
	// GetMappedObjectBundles returns a map of name -> bundle of objects from a specific table.
	GetMappedObjectBundles(ctx context.Context, tableName string, createObjBundleFunc bundle.CreateBundleFunction,
		createObjFunc bundle.CreateObjectFunction, getObjNameFunc bundle.ExtractObjectNameFunction,
	) (map[string]bundle.ObjectsBundle, *time.Time, error)
}

// ManagedClusterLabelsSpecDB is the interface needed by the spec transport bridge to sync managed-cluster labels table.
type ManagedClusterLabelsSpecDB interface {
	// GetUpdatedManagedClusterLabelsBundles returns a map of leaf-hub -> ManagedClusterLabelsSpecBundle of objects
	// belonging to a leaf-hub that had at least one update since the given timestamp, from a specific table.
	GetUpdatedManagedClusterLabelsBundles(ctx context.Context, tableName string,
		timestamp *time.Time) (map[string]*spec.ManagedClusterLabelsSpecBundle, error)
	// GetEntriesWithDeletedLabels returns a map of leaf-hub -> ManagedClusterLabelsSpecBundle of objects that have a
	// none-empty deleted-label-keys column.
	GetEntriesWithDeletedLabels(ctx context.Context,
		tableName string) (map[string]*spec.ManagedClusterLabelsSpecBundle, error)
	// UpdateDeletedLabelKeys updates
	UpdateDeletedLabelKeys(ctx context.Context, tableName string, readVersion int64, leafHubName string,
		managedClusterName string, deletedLabelKeys []string) error
	TempManagedClusterLabelsSpecDB
}

// ManagedClusterSetsSpecDB is the interface needed by the spec transport bridge to maintain managed-cluster-set tables.
// Currently, the spec-transport-bridge is the only reader/writer for the tracking table affected by this interface.
type ManagedClusterSetsSpecDB interface {
	// GetUpdatedManagedClusterSetsTracking returns a map of clusterSetName -> []leaf-hub-names from the respective
	// spec DB table. The entries are those that have been at least once updated since the given timestamp.
	GetUpdatedManagedClusterSetsTracking(ctx context.Context, tableName string,
		timestamp *time.Time) (map[string][]string, error)
	// AddManagedClusterSetTracking adds an entry that reflects the assignment of a managed-cluster into a
	// managed-cluster-set.
	AddManagedClusterSetTracking(ctx context.Context, tableName string, managedClusterSet string, leafHubName string,
		managedClusterName string) error
	// RemoveManagedClusterSetTracking removes an entry that reflects the assignment of a managed-cluster into a
	// managed-cluster-set.
	RemoveManagedClusterSetTracking(ctx context.Context, tableName string, managedClusterSet string, leafHubName string,
		managedClusterName string) error
}

// TempManagedClusterLabelsSpecDB appends ManagedClusterLabelsSpecDB interface with temporary functionality that should
// be removed after it is satisfied by a different component.
// TODO: once non-k8s-restapi exposes hub names, delete interface.
type TempManagedClusterLabelsSpecDB interface {
	// GetEntriesWithoutLeafHubName returns a slice of ManagedClusterLabelsSpec that are missing leaf hub name.
	GetEntriesWithoutLeafHubName(ctx context.Context, tableName string) ([]*spec.ManagedClusterLabelsSpec, error)
	// UpdateLeafHubName updates leaf hub name for a given managed cluster under optimistic concurrency.
	UpdateLeafHubName(ctx context.Context, tableName string, readVersion int64,
		managedClusterName string, leafHubName string) error
}

// StatusDB is the needed interface for the db transport bridge to fetch information from status DB.
type StatusDB interface {
	// GetManagedClusterLabelsStatus gets the labels present in managed-cluster CR metadata from a specific table.
	GetManagedClusterLabelsStatus(ctx context.Context, tableName string, leafHubName string,
		managedClusterName string) (map[string]string, error)
	TempStatusDB
}

// TempStatusDB appends StatusDB interface with temporary functionality that should be removed after it is satisfied
// by a different component.
// TODO: once non-k8s-restapi exposes hub names, delete interface.
type TempStatusDB interface {
	// GetManagedClusterLeafHubName returns leaf-hub name for a given managed cluster from a specific table.
	GetManagedClusterLeafHubName(ctx context.Context, tableName string, managedClusterName string) (string, error)
}
