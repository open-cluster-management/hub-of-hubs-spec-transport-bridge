package dbsyncer

import (
	"context"
	"fmt"
	"time"

	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-data-types/bundle/spec"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	managedClusterLabelsDBTableName = "managed_clusters_labels"
	managedClusterSetLabelKey       = "cluster.open-cluster-management.io/clusterset"
)

// AddManagedClusterLabelsDBToTransportSyncer adds managed-cluster labels db to transport syncer to the manager.
func AddManagedClusterLabelsDBToTransportSyncer(mgr ctrl.Manager, specDB db.SpecDB, transportObj transport.Transport,
	syncInterval time.Duration) error {
	lastSyncTimestampPtr := &time.Time{}

	if err := mgr.Add(&genericDBToTransportSyncer{
		log:            ctrl.Log.WithName("managed-cluster-labels-db-to-transport-syncer"),
		intervalPolicy: intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		syncBundleFunc: func(ctx context.Context) (bool, error) {
			return syncManagedClusterLabelsBundles(ctx, transportObj, datatypes.ManagedClustersLabelsMsgKey, specDB,
				managedClusterLabelsDBTableName, lastSyncTimestampPtr)
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
	lastUpdateTimestamp, err := specDB.GetLastUpdateTimestamp(ctx, dbTableName, false) // no resources in table
	if err != nil {
		return false, fmt.Errorf("unable to sync bundle - %w", err)
	}

	if !lastUpdateTimestamp.After(*lastSyncTimestampPtr) { // sync only if something has changed
		return false, nil
	}

	// if we got here, then the last update timestamp from db is after what we have in memory.
	// this means something has changed in db, syncing to transport.
	leafHubToLabelsSpecBundleMap,
		err := specDB.GetUpdatedManagedClusterLabelsBundles(ctx, dbTableName, lastSyncTimestampPtr)
	if err != nil {
		return false, fmt.Errorf("unable to sync bundle - %w", err)
	}

	// sync bundle per leaf hub
	for leafHubName, managedClusterLabelsBundle := range leafHubToLabelsSpecBundleMap {
		if err := syncToTransport(transportObj, leafHubName, transportBundleKey, lastUpdateTimestamp,
			managedClusterLabelsBundle); err != nil {
			return false, fmt.Errorf("unable to sync bundle to transport - %w", err)
		}
	}

	// track ManagedClusterSet assignments
	if err := trackManagedClusterSetAssignments(ctx, specDB, leafHubToLabelsSpecBundleMap); err != nil {
		return false, fmt.Errorf("unable to track managed cluster set label assignments - %w", err)
	}

	// updating value to retain same ptr between calls
	*lastSyncTimestampPtr = *lastUpdateTimestamp

	return true, nil
}

func trackManagedClusterSetAssignments(ctx context.Context, specDB db.SpecDB,
	leafHubToLabelsSpecBundleMap map[string]*spec.ManagedClusterLabelsSpecBundle) error {
	for leafHubName, managedClusterLabelsBundle := range leafHubToLabelsSpecBundleMap {
		for _, managedClusterLabelsSpec := range managedClusterLabelsBundle.Objects {
			// make sure MC is tracked if belongs to a set
			if clusterSetName, found := managedClusterLabelsSpec.Labels[managedClusterSetLabelKey]; found {
				// found a cluster-set, update tracking
				if err := specDB.AddManagedClusterSetTracking(ctx, managedClusterSetsTrackingTableName,
					clusterSetName, leafHubName, managedClusterLabelsSpec.ClusterName); err != nil {
					return fmt.Errorf("failed to track managed cluster set {%s} assignment for cluster {%s.%s} - %w",
						clusterSetName, leafHubName, managedClusterLabelsSpec.ClusterName, err)
				} // the un-tracking should later be supported
			}
		}
	}

	return nil
}
