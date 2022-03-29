package postgresql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/stolostron/hub-of-hubs-data-types/bundle/spec"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
)

var errOptimisticConcurrencyUpdateFailed = errors.New("zero rows were affected by an optimistic concurrency update")

// GetLastUpdateTimestamp returns the last update timestamp of a specific table.
func (p *PostgreSQL) GetLastUpdateTimestamp(ctx context.Context, tableName string) (*time.Time, error) {
	var lastTimestamp time.Time
	err := p.conn.QueryRow(ctx,
		fmt.Sprintf(`SELECT MAX(updated_at) FROM spec.%s`, tableName)).Scan(&lastTimestamp)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("no objects in the table spec.%s - %w", tableName, err)
	}

	return &lastTimestamp, nil
}

// GetObjectsBundle returns a bundle of objects from a specific table.
func (p *PostgreSQL) GetObjectsBundle(ctx context.Context, tableName string, createObjFunc bundle.CreateObjectFunction,
	intoBundle bundle.ObjectsBundle) (*time.Time, error) {
	timestamp, err := p.GetLastUpdateTimestamp(ctx, tableName)
	if err != nil {
		return nil, err
	}

	rows, err := p.conn.Query(ctx, fmt.Sprintf(`SELECT id,payload,deleted FROM spec.%s`, tableName))
	defer rows.Close()

	if err != nil {
		return nil, fmt.Errorf("failed to query table spec.%s - %w", tableName, err)
	}

	for rows.Next() {
		var (
			objID   string
			deleted bool
		)

		object := createObjFunc()
		if err := rows.Scan(&objID, &object, &deleted); err != nil {
			return nil, fmt.Errorf("error reading from table spec.%s - %w", tableName, err)
		}

		if deleted {
			intoBundle.AddDeletedObject(object)
		} else {
			intoBundle.AddObject(object, objID)
		}
	}

	return timestamp, nil
}

// GetMappedObjectBundles returns a map of name -> bundle of objects from a specific table.
func (p *PostgreSQL) GetMappedObjectBundles(ctx context.Context, tableName string,
	createObjBundleFunc bundle.CreateBundleFunction, createObjFunc bundle.CreateObjectFunction,
	getObjNameFunc bundle.ExtractObjectNameFunction) (map[string]bundle.ObjectsBundle, *time.Time, error) {
	timestamp, err := p.GetLastUpdateTimestamp(ctx, tableName)
	if err != nil {
		return nil, nil, err
	}

	mappedObjectBundles := map[string]bundle.ObjectsBundle{}

	rows, err := p.conn.Query(ctx, fmt.Sprintf(`SELECT id,payload,deleted FROM spec.%s`, tableName))
	defer rows.Close()

	if err != nil {
		return nil, nil, fmt.Errorf("failed to query table spec.%s - %w", tableName, err)
	}

	for rows.Next() {
		var (
			objID   string
			deleted bool
		)

		object := createObjFunc()
		if err := rows.Scan(&objID, &object, &deleted); err != nil {
			return nil, nil, fmt.Errorf("error reading from table spec.%s - %w", tableName, err)
		}

		// create bundle if not mapped
		key := getObjNameFunc(object)
		if _, found := mappedObjectBundles[key]; !found {
			mappedObjectBundles[key] = createObjBundleFunc()
		}

		if deleted {
			mappedObjectBundles[key].AddDeletedObject(object)
		} else {
			mappedObjectBundles[key].AddObject(object, objID)
		}
	}

	return mappedObjectBundles, timestamp, nil
}

// GetUpdatedManagedClusterLabelsBundles returns a map of leaf-hub -> ManagedClusterLabelsSpecBundle of objects
// belonging to a leaf-hub that had at least once update since the given timestamp, from a specific table.
func (p *PostgreSQL) GetUpdatedManagedClusterLabelsBundles(ctx context.Context, tableName string,
	timestamp *time.Time) (map[string]*spec.ManagedClusterLabelsSpecBundle, *time.Time, error) {
	latestTimestampInTable, err := p.GetLastUpdateTimestamp(ctx, tableName)
	if err != nil {
		return nil, nil, err
	}

	// select ManagedClusterLabelsSpec entries information from DB
	rows, err := p.conn.Query(ctx, fmt.Sprintf(`SELECT leaf_hub_name,managed_cluster_name,labels,
deleted_label_keys,updated_at,version FROM spec.%s WHERE leaf_hub_name IN (SELECT DISTINCT(leaf_hub_name) 
from spec.%s WHERE updated_at::timestamp > timestamp '%s')`, tableName, tableName, timestamp.Format(time.RFC3339Nano)))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query table spec.%s - %w", tableName, err)
	}

	leafHubToLabelsSpecBundleMap, err := p.getLabelsSpecBundlesFromRows(rows)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get managed cluster labels bundles - %w", err)
	}

	return leafHubToLabelsSpecBundleMap, latestTimestampInTable, nil
}

// GetEntriesWithDeletedLabels returns a map of leaf-hub -> ManagedClusterLabelsSpecBundle of objects that have a
// none-empty deleted-label-keys column.
func (p *PostgreSQL) GetEntriesWithDeletedLabels(ctx context.Context,
	tableName string) (map[string]*spec.ManagedClusterLabelsSpecBundle, error) {
	rows, _ := p.conn.Query(ctx, fmt.Sprintf(`SELECT leaf_hub_name,managed_cluster_name,labels,
deleted_label_keys,updated_at,version FROM spec.%s WHERE deleted_label_keys != '[]'`, tableName))
	defer rows.Close()

	leafHubToLabelsSpecBundleMap, err := p.getLabelsSpecBundlesFromRows(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to get managed cluster spec entries with deleted labels - %w", err)
	}

	return leafHubToLabelsSpecBundleMap, nil
}

func (p *PostgreSQL) getLabelsSpecBundlesFromRows(rows pgx.Rows) (map[string]*spec.ManagedClusterLabelsSpecBundle,
	error) {
	leafHubToLabelsSpecBundleMap := make(map[string]*spec.ManagedClusterLabelsSpecBundle)

	for rows.Next() {
		var (
			leafHubName        string
			managedClusterName string
			labels             map[string]string
			deletedLabelKeys   []string
			updatedAt          time.Time
			version            int64
		)

		if err := rows.Scan(&leafHubName, &managedClusterName, &labels, &deletedLabelKeys, &updatedAt,
			&version); err != nil {
			return nil, fmt.Errorf("error reading from table - %w", err)
		}

		// create ManagedClusterLabelsSpecBundle if not mapped for leafHub
		managedClusterLabelsSpecBundle, found := leafHubToLabelsSpecBundleMap[leafHubName]
		if !found {
			managedClusterLabelsSpecBundle = &spec.ManagedClusterLabelsSpecBundle{
				Objects:     []*spec.ManagedClusterLabelsSpec{},
				LeafHubName: leafHubName,
			}

			leafHubToLabelsSpecBundleMap[leafHubName] = managedClusterLabelsSpecBundle
		}

		// append entry to bundle
		managedClusterLabelsSpecBundle.Objects = append(managedClusterLabelsSpecBundle.Objects,
			&spec.ManagedClusterLabelsSpec{
				Name:             managedClusterName,
				Labels:           labels,
				DeletedLabelKeys: deletedLabelKeys,
				UpdateTimestamp:  updatedAt,
				Version:          version,
			})
	}

	return leafHubToLabelsSpecBundleMap, nil
}

// UpdateDeletedLabelKeysOptimistically updates deleted_label_keys value for a managed cluster entry under
// optimistic concurrency approach.
func (p *PostgreSQL) UpdateDeletedLabelKeysOptimistically(ctx context.Context, tableName string, readVersion int64,
	leafHubName string, managedClusterName string, deletedLabelKeys []string) error {
	exists := false
	if err := p.conn.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS(SELECT 1 from spec.%s WHERE leaf_hub_name=$1 AND 
			managed_cluster_name=$2 AND version=$3)`, tableName), leafHubName, managedClusterName,
		readVersion).Scan(&exists); err != nil {
		return fmt.Errorf("failed to read from spec.%s - %w", tableName, err)
	}

	if exists { // row for (leaf hub, mc, version) tuple exists, update the db.
		deletedLabelsJSON, err := json.Marshal(deletedLabelKeys)
		if err != nil {
			return fmt.Errorf("failed to marshal deleted labels - %w", err)
		}

		if commandTag, err := p.conn.Exec(ctx, fmt.Sprintf(`UPDATE spec.%s SET updated_at=now(),deleted_label_keys=$1,version=$2 
		WHERE leaf_hub_name=$3 AND managed_cluster_name=$4 AND version=$5`, tableName), deletedLabelsJSON,
			readVersion+1, leafHubName, managedClusterName, readVersion); err != nil {
			return fmt.Errorf("failed to update managed cluster labels row in spec.%s - %w", tableName, err)
		} else if commandTag.RowsAffected() == 0 {
			return errOptimisticConcurrencyUpdateFailed
		}
	}

	return nil
}

// GetUpdatedManagedClusterSetsTracking returns a slice of ManagedClusterSet-tracking entries from the respective
// spec DB table. The entries are those that have been at least once updated since the given timestamp.
func (p *PostgreSQL) GetUpdatedManagedClusterSetsTracking(ctx context.Context, tableName string,
	timestamp *time.Time) (map[string][]string, *time.Time, error) {
	latestTimestampInTable, err := p.GetLastUpdateTimestamp(ctx, tableName)
	if err != nil {
		return nil, nil, err
	}

	// build mapping from entries in DB (don't need MCs)
	rows, err := p.conn.Query(ctx, fmt.Sprintf(`SELECT cluster_set_name,leaf_hub_name FROM 
		spec.%s WHERE updated_at::timestamp > timestamp '%s'`, tableName, timestamp.Format(time.RFC3339Nano)))
	defer rows.Close()

	if err != nil {
		return nil, nil, fmt.Errorf("failed to query table spec.%s - %w", tableName, err)
	}

	clusterSetToLeafHubsMap := map[string][]string{}

	for rows.Next() {
		var (
			clusterSetName string
			leafHubName    string
		)

		if err := rows.Scan(&clusterSetName, &leafHubName); err != nil {
			return nil, nil, fmt.Errorf("error reading from table - %w", err)
		}
		// create mapping or append (uniques) if not found
		if uniqueLeafHubs, found := clusterSetToLeafHubsMap[clusterSetName]; !found {
			clusterSetToLeafHubsMap[clusterSetName] = []string{leafHubName}
		} else {
			clusterSetToLeafHubsMap[clusterSetName] = append(uniqueLeafHubs, leafHubName) // unique DB index
		}
	}

	return clusterSetToLeafHubsMap, latestTimestampInTable, nil
}

// AddManagedClusterSetTracking adds an entry that reflects the assignment of a managed-cluster into a
// managed-cluster-set.
//nolint
func (p *PostgreSQL) AddManagedClusterSetTracking(ctx context.Context, tableName string, clusterSetName string,
	leafHubName string, managedClusterName string) error {
	exists, err := p.managedClusterSetTrackingExists(ctx, tableName, clusterSetName)
	if err != nil {
		return fmt.Errorf("failed to check if managed cluster set tracking exists - %w", err)
	}

	if !exists { // doesn't exist, insert entry
		managedClusterJSON, err := json.Marshal([]string{managedClusterName})
		if err != nil {
			return fmt.Errorf("failed to marshal managed-clusters slice - %w", err)
		}

		if _, err := p.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO spec.%s (cluster_set_name,leaf_hub_name,
			managed_clusters, updated_at) values($1, $2, $3, now())`, tableName), clusterSetName, leafHubName,
			managedClusterJSON); err != nil {
			return fmt.Errorf("failed to insert into database: %w", err)
		}

		return nil
	}

	// read MCS, LH entry to update it
	rows, err := p.conn.Query(ctx, fmt.Sprintf(`SELECT managed_clusters FROM spec.%s WHERE 
		cluster_set_name=$1 AND leaf_hub_name=$2`, tableName), clusterSetName, leafHubName)
	defer rows.Close()

	if err != nil {
		return fmt.Errorf("failed to read from spec.%s - %w", tableName, err)
	}

	if rows.Next() {
		var managedClusters []string
		if err := rows.Scan(&managedClusters); err != nil {
			return fmt.Errorf("error reading from spec.%s - %w", tableName, err)
		}

		// check if exists already
		for _, managedCluster := range managedClusters {
			if managedCluster == managedClusterName {
				return nil // already tracked
			}
		}
		// otherwise, append cluster
		managedClusters = append(managedClusters, managedClusterName)

		// update row
		if err := p.updateManagedClusterSetTrackingEntry(ctx, tableName, clusterSetName, leafHubName,
			managedClusters); err != nil {
			return fmt.Errorf("failed to update managed cluster set tracking entry - %w", err)
		}
	}

	return nil
}

// RemoveManagedClusterSetTracking removes an entry that reflects the assignment of a managed-cluster into a
// managed-cluster-set.
func (p *PostgreSQL) RemoveManagedClusterSetTracking(ctx context.Context, tableName string, clusterSetName string,
	leafHubName string, managedClusterName string) error {
	exists, err := p.managedClusterSetTrackingExists(ctx, tableName, clusterSetName)
	if err != nil {
		return fmt.Errorf("failed to check if managed cluster set tracking exists - %w", err)
	}

	if !exists {
		return nil // doesn't exist, nothing to remove
	}

	// read MCS, LH entry to update it
	rows, err := p.conn.Query(ctx, fmt.Sprintf(`SELECT managed_clusters FROM spec.%s WHERE 
		cluster_set_name=$1 AND leaf_hub_name=$2`, tableName), clusterSetName, leafHubName)
	defer rows.Close()

	if err != nil {
		return fmt.Errorf("failed to read from spec.%s - %w", tableName, err)
	}

	if rows.Next() {
		var managedClusters []string
		if err := rows.Scan(&managedClusters); err != nil {
			return fmt.Errorf("error reading from spec.%s - %w", tableName, err)
		}

		// check if managed cluster exists (remove if it does)
		managedClusterExists := false

		for idx, managedCluster := range managedClusters {
			if managedCluster == managedClusterName {
				managedClusterExists = true

				managedClusters = append(managedClusters[:idx], managedClusters[idx+1:]...)
			}
		}

		if !managedClusterExists {
			return nil // not found, nothing to remove
		}

		// update row with removed cluster (does not delete if mapping is empty array intentionally, so that a track
		// remains in-case something such as deleting a CR must happen in the future)
		if err := p.updateManagedClusterSetTrackingEntry(ctx, tableName, clusterSetName, leafHubName,
			managedClusters); err != nil {
			return fmt.Errorf("failed to update managed cluster set tracking entry - %w", err)
		}
	}

	return nil
}

func (p *PostgreSQL) managedClusterSetTrackingExists(ctx context.Context, tableName string,
	clusterSetName string) (bool, error) {
	exists := false
	if err := p.conn.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS(SELECT 1 from spec.%s WHERE cluster_set_name=$1)`,
		tableName), clusterSetName).Scan(&exists); err != nil {
		return false, fmt.Errorf("failed to read from spec.%s - %w", tableName, err)
	}

	return exists, nil
}

func (p *PostgreSQL) updateManagedClusterSetTrackingEntry(ctx context.Context, tableName string, clusterSetName string,
	leafHubName string, managedClusters []string) error {
	managedClustersJSON, err := json.Marshal(managedClusters)
	if err != nil {
		return fmt.Errorf("failed to marshal managed-clusters slice - %w", err)
	}

	if _, err := p.conn.Exec(ctx, fmt.Sprintf(`UPDATE spec.%s SET updated_at=now(),managed_clusters=$1 
				WHERE cluster_set_name=$2 AND leaf_hub_name=$3`, tableName), managedClustersJSON, clusterSetName,
		leafHubName); err != nil {
		return fmt.Errorf("failed to update managed cluster set tracking row in spec.%s - %w", tableName, err)
	}

	return nil
}

// GetEntriesWithoutLeafHubName returns a slice of ManagedClusterLabelsSpec that are missing leaf hub name.
func (p *PostgreSQL) GetEntriesWithoutLeafHubName(ctx context.Context,
	tableName string) ([]*spec.ManagedClusterLabelsSpec, error) {
	rows, err := p.conn.Query(ctx, fmt.Sprintf(`SELECT managed_cluster_name, version FROM spec.%s WHERE 
		leaf_hub_name = ''`, tableName))
	defer rows.Close()

	if err != nil {
		return nil, fmt.Errorf("failed to read from spec.%s - %w", tableName, err)
	}

	managedClusterLabelsSpecSlice := make([]*spec.ManagedClusterLabelsSpec, 0)

	for rows.Next() {
		var (
			managedClusterName string
			version            int64
		)

		if err := rows.Scan(&managedClusterName, &version); err != nil {
			return nil, fmt.Errorf("error reading from spec.%s - %w", tableName, err)
		}

		managedClusterLabelsSpecSlice = append(managedClusterLabelsSpecSlice, &spec.ManagedClusterLabelsSpec{
			Name:    managedClusterName,
			Version: version,
		})
	}

	return managedClusterLabelsSpecSlice, nil
}

// UpdateLeafHubNamesOptimistically updates leaf hub name for a given managed cluster under optimistic concurrency.
func (p *PostgreSQL) UpdateLeafHubNamesOptimistically(ctx context.Context, tableName string, readVersion int64,
	managedClusterName string, leafHubName string) error {
	if commandTag, err := p.conn.Exec(ctx, fmt.Sprintf(`UPDATE spec.%s SET updated_at=now(),leaf_hub_name=$1,version=$2 
				WHERE managed_cluster_name=$3 AND version=$4`, tableName), leafHubName, readVersion+1,
		managedClusterName, readVersion); err != nil {
		return fmt.Errorf("failed to update managed cluster labels row in spec.%s - %w", tableName, err)
	} else if commandTag.RowsAffected() == 0 {
		return errOptimisticConcurrencyUpdateFailed
	}

	return nil
}
