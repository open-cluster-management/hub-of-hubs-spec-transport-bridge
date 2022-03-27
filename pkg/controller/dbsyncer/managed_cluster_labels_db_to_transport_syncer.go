package dbsyncer

import (
	"context"
	"encoding/json"
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
func AddManagedClusterLabelsDBToTransportSyncer(mgr ctrl.Manager, db db.SpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	dbToTransportSyncer := &managedClusterLabelsDBToTransportSyncer{
		genericDBToTransportSyncer: &genericDBToTransportSyncer{
			log:                ctrl.Log.WithName("managed-cluster-labels-db-to-transport-syncer"),
			db:                 db,
			dbTableName:        managedClusterLabelsDBTableName,
			transport:          transport,
			transportBundleKey: datatypes.ManagedClustersMetadataMsgKey,
			intervalPolicy:     intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		},
	}

	dbToTransportSyncer.syncBundleFunc = dbToTransportSyncer.syncManagedClusterLabelsBundles

	if err := mgr.Add(dbToTransportSyncer); err != nil {
		return fmt.Errorf("failed to add managed-cluster labels db to transport syncer - %w", err)
	}

	return nil
}

type managedClusterLabelsDBToTransportSyncer struct {
	*genericDBToTransportSyncer
}

// syncManagedClusterLabelsBundles performs the actual sync logic and returns true if bundle was committed to transport,
// otherwise false.
func (syncer *managedClusterLabelsDBToTransportSyncer) syncManagedClusterLabelsBundles(ctx context.Context) bool {
	lastUpdateTimestamp, err := syncer.db.GetLastUpdateTimestamp(ctx, syncer.dbTableName)
	if err != nil {
		syncer.log.Error(err, "unable to sync bundle to leaf hubs", "tableName", syncer.dbTableName)

		return false
	}

	if !lastUpdateTimestamp.After(*syncer.lastUpdateTimestamp) { // sync only if something has changed
		return false
	}

	// if we got here, then the last update timestamp from db is after what we have in memory.
	// this means something has changed in db, syncing to transport.
	leafHubToLabelsSpecBundleMap, lastUpdateTimestamp,
		err := syncer.db.GetUpdatedManagedClusterLabelsBundles(ctx, syncer.dbTableName, syncer.lastUpdateTimestamp)
	if err != nil {
		syncer.log.Error(err, "unable to sync bundle to leaf hubs", "tableName", syncer.dbTableName)

		return false
	}
	// remove entries with no LH name (temporary state)
	delete(leafHubToLabelsSpecBundleMap, "") // TODO: once non-k8s-restapi exposes hub names, remove line.
	// track ManagedClusterSet assignments
	if err := syncer.trackManagedClusterSetAssignments(ctx, leafHubToLabelsSpecBundleMap); err != nil {
		syncer.log.Error(err, "unable to track managed cluster set label assignments",
			"tableName", managedClusterSetsTrackingTableName)

		return false
	}

	syncer.lastUpdateTimestamp = lastUpdateTimestamp

	// sync bundle per leaf hub
	for leafHubName, managedClusterLabelsBundle := range leafHubToLabelsSpecBundleMap {
		syncer.syncToTransport(leafHubName, syncer.transportBundleKey, datatypes.SpecBundle, lastUpdateTimestamp,
			managedClusterLabelsBundle)
	}

	return true
}

func (syncer *managedClusterLabelsDBToTransportSyncer) syncToTransport(destination string, objID string, objType string,
	timestamp *time.Time, payload *spec.ManagedClusterLabelsSpecBundle) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		syncer.log.Error(err, "failed to sync object", "objectId", objID, "objectType", objType)
		return
	}

	syncer.transport.SendAsync(destination, objID, objType, timestamp.Format(timeFormat), payloadBytes)
}

func (syncer *managedClusterLabelsDBToTransportSyncer) trackManagedClusterSetAssignments(ctx context.Context,
	leafHubToLabelsSpecBundleMap map[string]*spec.ManagedClusterLabelsSpecBundle) error {
	for leafHubName, managedClusterLabelsBundle := range leafHubToLabelsSpecBundleMap {
		for _, managedClusterLabelsSpec := range managedClusterLabelsBundle.Objects {
			// make sure MC is tracked if belongs to a set
			if clusterSetName, found := managedClusterLabelsSpec.Labels[managedClusterSetLabelKey]; found {
				// found a cluster-set, update tracking
				if err := syncer.db.AddManagedClusterSetTracking(ctx, managedClusterSetsTrackingTableName,
					clusterSetName, leafHubName, managedClusterLabelsSpec.Name); err != nil {
					return fmt.Errorf("failed to track managed cluster set {%s} assignment for cluster {%s.%s} - %w",
						clusterSetName, leafHubName, managedClusterLabelsSpec.Name, err)
				} // the un-tracking should later be supported
			}
		}
	}

	return nil
}
