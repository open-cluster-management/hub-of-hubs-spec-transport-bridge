package dbsyncer

import (
	"context"
	"fmt"
	"time"

	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

const managedClusterSetsTrackingTableName = "managed_cluster_sets_tracking"

// AddManagedClusterSetsTrackingDBToTransportSyncer adds managed-cluster-sets-tracking db to transport syncer to the
// manager. The syncer watches an MCS, LH -> MCs table and syncs Objects from two different tables when the first is
// changed, per leaf-hub.
func AddManagedClusterSetsTrackingDBToTransportSyncer(mgr ctrl.Manager, specDB db.SpecDB,
	transportObj transport.Transport, syncInterval time.Duration) error {
	lastSyncTimestampPtr := &time.Time{}

	if err := mgr.Add(&genericDBToTransportSyncer{
		log:            ctrl.Log.WithName("managed-cluster-sets-tracking-db-to-transport-syncer"),
		intervalPolicy: intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		syncBundleFunc: func(ctx context.Context) (bool, error) {
			return syncManagedClusterSetResourcesBasedOnTracking(ctx, transportObj, specDB,
				managedClusterSetsTrackingTableName, lastSyncTimestampPtr, true, true)
		},
	}); err != nil {
		return fmt.Errorf("failed to add managed-cluster-sets tracking db to transport syncer - %w", err)
	}

	return nil
}

func syncManagedClusterSetResourcesBasedOnTracking(ctx context.Context, transportObj transport.Transport,
	specDB db.SpecDB, dbTableName string, lastSyncTimestampPtr *time.Time, syncManagedClusterSets bool,
	syncManagedClusterSetBindings bool) (bool, error) {
	lastUpdateTimestamp, err := specDB.GetLastUpdateTimestamp(ctx, dbTableName, false)
	if err != nil {
		return false, fmt.Errorf("unable to sync bundle - %w", err)
	}

	if !lastUpdateTimestamp.After(*lastSyncTimestampPtr) { // sync only if something has changed
		return false, nil
	}

	// if we got here, then the last update timestamp from db is after what we have in memory.
	// this means something has changed in db, get updated MCS tracking and sync MCS objects to transport.
	clusterSetToLeafHubsMap, err := specDB.GetUpdatedManagedClusterSetsTracking(ctx, dbTableName, lastSyncTimestampPtr)
	if err != nil {
		return false, fmt.Errorf("unable to sync bundle to leaf hubs: failed to get MCS tracking - %w", err)
	}

	if syncManagedClusterSetBindings {
		// sync LH -> MCS-binding bundles
		if err := syncObjectBundlesBasedOnManagedClusterSetTracking(ctx, transportObj, managedClusterSetBindingsMsgKey,
			specDB, managedClusterSetBindingsTableName, bundle.NewBaseObjectsBundle,
			createManagedClusterSetBindingObjFunc, clusterSetToLeafHubsMap); err != nil {
			return false, fmt.Errorf("failed to sync managed-cluster-sets bundles - %w", err)
		}
	}

	if syncManagedClusterSets {
		// sync LH -> MCS bundles
		if err := syncObjectBundlesBasedOnManagedClusterSetTracking(ctx, transportObj, managedClusterSetsMsgKey, specDB,
			managedClusterSetsTrackingTableName, bundle.NewBaseObjectsBundle, createManagedClusterSetObjFunc,
			clusterSetToLeafHubsMap); err != nil {
			return false, fmt.Errorf("failed to sync managed-cluster-sets bundles - %w", err)
		}
	}

	// updating value to retain same ptr between calls
	*lastSyncTimestampPtr = *lastUpdateTimestamp

	return true, nil
}
