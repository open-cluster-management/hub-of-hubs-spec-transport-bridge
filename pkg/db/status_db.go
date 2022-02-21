package db

import "context"

// StatusDB is the needed interface for the db transport bridge to fetch information from status DB.
type StatusDB interface {
	// GetManagedClusterLabelsStatus gets the labels present in managed-cluster CR metadata from a specific table.
	GetManagedClusterLabelsStatus(ctx context.Context, tableName string, leafHubName string,
		managedClusterName string) (map[string]string, error)
	// Stop stops db and releases resources (e.g. connection pool).
	Stop()
	TempStatusDB
}

// TempStatusDB appends StatusDB interface with temporary functionality that should be removed after it is satisfied
// by a different component.
// TODO: once non-k8s-restapi exposes hub names, delete interface.
type TempStatusDB interface {
	// GetManagedClusterLeafHubName returns leaf-hub name for a given managed cluster from a specific table.
	GetManagedClusterLeafHubName(ctx context.Context, tableName string, managedClusterName string) (string, error)
}
