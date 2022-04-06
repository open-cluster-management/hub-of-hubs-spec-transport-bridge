package dbsyncer

import (
	"context"
	"fmt"
	"time"

	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/helpers"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"open-cluster-management.io/api/cluster/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const managedClusterSetsTrackingTableName = "managed_cluster_sets_tracking"

// AddManagedClusterSetsTrackingDBToTransportSyncer adds managed-cluster-sets-tracking db to transport syncer to the
// manager. The syncer watches an MCS, LH -> MCs table and syncs Objects from two different tables when the first is
// changed, per leaf-hub.
func AddManagedClusterSetsTrackingDBToTransportSyncer(mgr ctrl.Manager, db db.SpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	dbToTransportSyncer := &managedClusterSetsTrackingDBToTransportSyncer{
		genericDBToTransportSyncer: &genericDBToTransportSyncer{
			log:            ctrl.Log.WithName("managed-cluster-sets-tracking-db-to-transport-syncer"),
			db:             db,
			dbTableName:    managedClusterSetsTrackingTableName, // reconcile by tracking table
			transport:      transport,
			intervalPolicy: intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		},
		createManagedClusterSetObjFunc: func() metav1.Object { return &v1beta1.ManagedClusterSet{} },
		createManagedClusterSetBindingObjFunc: func() metav1.Object {
			return &v1beta1.ManagedClusterSetBinding{}
		},
		createBundleFunc: bundle.NewBaseBundle,
	}

	dbToTransportSyncer.syncBundleFunc = dbToTransportSyncer.syncObjectsPerLeafHub

	if err := mgr.Add(dbToTransportSyncer); err != nil {
		return fmt.Errorf("failed to add managed-cluster labels db to transport syncer - %w", err)
	}

	return nil
}

type managedClusterSetsTrackingDBToTransportSyncer struct {
	*genericDBToTransportSyncer
	createManagedClusterSetObjFunc        bundle.CreateObjectFunction
	createManagedClusterSetBindingObjFunc bundle.CreateObjectFunction
	createBundleFunc                      bundle.CreateBundleFunction
}

func (syncer *managedClusterSetsTrackingDBToTransportSyncer) syncObjectsPerLeafHub(ctx context.Context) bool {
	lastUpdateTimestamp, err := syncer.db.GetLastUpdateTimestamp(ctx, syncer.dbTableName)
	if err != nil {
		syncer.log.Error(err, "unable to sync bundle to leaf hubs - failed to get timestamp",
			"tableName", syncer.dbTableName)

		return false
	}

	if !lastUpdateTimestamp.After(*syncer.lastUpdateTimestamp) { // sync only if something has changed
		return false
	}

	// if we got here, then the last update timestamp from db is after what we have in memory.
	// this means something has changed in db, get updated MCS tracking and sync MCS objects to transport.
	clusterSetToLeafHubsMap, _,
		err := syncer.db.GetUpdatedManagedClusterSetsTracking(ctx, syncer.dbTableName, syncer.lastUpdateTimestamp)
	if err != nil {
		syncer.log.Error(err, "unable to sync bundle to leaf hubs - failed to get MCS tracking",
			"tableName", syncer.dbTableName)

		return false
	}

	// get LH -> MCS-binding bundles
	managedClusterSetBindingBundles, err := syncer.getManagedClusterSetBindingsBundles(ctx, clusterSetToLeafHubsMap)
	if err != nil {
		syncer.log.Error(err, "failed to get managed-cluster-set bundles",
			"tableName", syncer.dbTableName)

		return false
	}

	// get LH -> MCS bundles
	managedClusterSetBundles, err := syncer.getManagedClusterSetsBundles(ctx, clusterSetToLeafHubsMap)
	if err != nil {
		syncer.log.Error(err, "failed to get managed-cluster-set-binding bundles",
			"tableName", syncer.dbTableName)

		return false
	}

	syncer.lastUpdateTimestamp = lastUpdateTimestamp

	// sync bindings
	for leafHub, objectsBundle := range managedClusterSetBindingBundles {
		syncer.syncToTransport(leafHub, managedClusterSetBindingsMsgKey, datatypes.SpecBundle,
			lastUpdateTimestamp, objectsBundle)
	}

	// sync sets
	for leafHub, objectsBundle := range managedClusterSetBundles {
		syncer.syncToTransport(leafHub, managedClusterSetsMsgKey, datatypes.SpecBundle, lastUpdateTimestamp,
			objectsBundle)
	}

	return true
}

func (syncer *managedClusterSetsTrackingDBToTransportSyncer) getManagedClusterSetsBundles(ctx context.Context,
	clusterSetToLeafHubsMap map[string][]string) (map[string]bundle.ObjectsBundle, error) {
	// get MCS bundles mapped by MCS name
	mappedManagedClusterSetBundles, _, err := syncer.db.GetMappedObjectBundles(ctx, managedClusterSetsTableName,
		syncer.createBundleFunc, syncer.createManagedClusterSetObjFunc, func(obj metav1.Object) string {
			return obj.GetName()
		})
	if err != nil {
		return nil, fmt.Errorf("failed to get mapped object bundles from DB - %w", err)
	}

	// build leaf-hub -> MCS bundles map
	leafHubToObjectsBundleMap, err := syncer.buildLeafHubToObjectsBundleMap(ctx, clusterSetToLeafHubsMap,
		mappedManagedClusterSetBundles)
	if err != nil {
		return nil, fmt.Errorf("unable to sync bundle to leaf hubs - failed to build objects map from DB - %w", err)
	}

	return leafHubToObjectsBundleMap, nil
}

func (syncer *managedClusterSetsTrackingDBToTransportSyncer) getManagedClusterSetBindingsBundles(ctx context.Context,
	clusterSetToLeafHubsMap map[string][]string) (map[string]bundle.ObjectsBundle, error) {
	// get MCS-binding bundles mapped by MCS name
	mappedManagedClusterSetBindingBundles, _, err := syncer.db.GetMappedObjectBundles(ctx,
		managedClusterSetBindingsTableName, syncer.createBundleFunc,
		syncer.createManagedClusterSetBindingObjFunc, func(obj metav1.Object) string {
			return obj.GetName()
		})
	if err != nil {
		return nil, fmt.Errorf("failed to get mapped object bundles from DB - %w", err)
	}

	// if there are no bindings, don't proceed
	if len(mappedManagedClusterSetBindingBundles) == 0 {
		return mappedManagedClusterSetBindingBundles, nil
	}

	// build leaf-hub -> MCS-binding bundles map
	leafHubToObjectsBundleMap, err := syncer.buildLeafHubToObjectsBundleMap(ctx, clusterSetToLeafHubsMap,
		mappedManagedClusterSetBindingBundles)
	if err != nil {
		return nil, fmt.Errorf("unable to sync bundle to leaf hubs - failed to build objects map from DB - %w", err)
	}

	return leafHubToObjectsBundleMap, nil
}

func (syncer *managedClusterSetsTrackingDBToTransportSyncer) buildLeafHubToObjectsBundleMap(_ context.Context,
	clusterSetToLeafHubsMap map[string][]string,
	mappedObjectBundles map[string]bundle.ObjectsBundle) (map[string]bundle.ObjectsBundle, error) {
	leafHubToObjectsBundleMap := map[string]bundle.ObjectsBundle{}
	// build lh -> MCS bundle
	for clusterSetName, LeafHubs := range clusterSetToLeafHubsMap {
		objectsBundle, found := mappedObjectBundles[clusterSetName]
		if !found {
			continue
		}
		// add objects bundle for all leaf hubs in mapping
		for _, leafHub := range LeafHubs {
			// create mapping if it doesn't exist
			if _, found := leafHubToObjectsBundleMap[leafHub]; !found {
				leafHubToObjectsBundleMap[leafHub] = syncer.createBundleFunc()
			}
			// merge content
			if err := leafHubToObjectsBundleMap[leafHub].MergeBundle(objectsBundle); err != nil {
				return nil, fmt.Errorf("failed to merge object bundles : clusterSetName=%s, leafHubName=%s - %w",
					clusterSetName, leafHub, err)
			}
		}
	}

	return leafHubToObjectsBundleMap, nil
}

func (syncer *managedClusterSetsTrackingDBToTransportSyncer) syncToTransport(destination string, objID string,
	objType string, timestamp *time.Time, payload bundle.ObjectsBundle) {
	if err := helpers.SyncObjectsToTransport(syncer.transport, destination, objID, objType, timestamp,
		payload); err != nil {
		syncer.log.Error(err, "failed to sync object", "objectId", objID, "objectType", objType)
	}
}
