package dbsyncer

import (
	"context"
	"fmt"
	"time"

	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

const managedClusterLabelsDBTableName = "managed_clusters_labels"

// AddManagedClusterLabelsDBToTransportSyncer adds managed-cluster labels db to transport syncer to the manager.
func AddManagedClusterLabelsDBToTransportSyncer(mgr ctrl.Manager, specDB db.SpecDB, transportObj transport.Transport,
	syncInterval time.Duration) error {
	if err := mgr.Add(&genericDBToTransportSyncer{
		log:            ctrl.Log.WithName("managed-cluster-labels-db-to-transport-syncer"),
		intervalPolicy: intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		syncBundleFunc: func(ctx context.Context) (bool, error) {
			return syncManagedClusterLabelsBundles(ctx, transportObj, datatypes.ManagedClustersLabelsMsgKey, specDB,
				managedClusterLabelsDBTableName, &time.Time{})
		},
	}); err != nil {
		return fmt.Errorf("failed to add managed-cluster labels db to transport syncer - %w", err)
	}

	return nil
}

// syncManagedClusterLabelsBundles performs the actual sync logic and returns true if bundle was committed to transport,
// otherwise false.
func syncManagedClusterLabelsBundles(ctx context.Context, transportObj transport.Transport, transportBundleKey string,
	specDB db.SpecDB, dbTableName string, lastSyncTimestampPtr *time.Time) (bool, error) {
	lastUpdateTimestamp, err := specDB.GetLastUpdateTimestamp(ctx, dbTableName)
	if err != nil {
		return false, fmt.Errorf("unable to sync bundle - %w", err)
	}

	if !lastUpdateTimestamp.After(*lastSyncTimestampPtr) { // sync only if something has changed
		return false, nil
	}

	// if we got here, then the last update timestamp from db is after what we have in memory.
	// this means something has changed in db, syncing to transport.
	leafHubToLabelsSpecBundleMap, _,
		err := specDB.GetUpdatedManagedClusterLabelsBundles(ctx, dbTableName, lastUpdateTimestamp)
	if err != nil {
		return false, fmt.Errorf("unable to sync bundle - %w", err)
	}
	// remove entries with no LH name (temporary state)
	delete(leafHubToLabelsSpecBundleMap, "") // TODO: once non-k8s-restapi exposes hub names, remove line.

	*lastSyncTimestampPtr = *lastUpdateTimestamp

	// sync bundle per leaf hub
	for leafHubName, managedClusterLabelsBundle := range leafHubToLabelsSpecBundleMap {
		if err := syncToTransport(transportObj, leafHubName, transportBundleKey, datatypes.SpecBundle,
			lastUpdateTimestamp, managedClusterLabelsBundle); err != nil {
			return false, fmt.Errorf("unable to sync bundle to transport - %w", err)
		}
	}

	return true, nil
}
