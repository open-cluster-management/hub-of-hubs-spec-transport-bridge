package dbsyncer

import (
	"context"
	"fmt"
	"time"

	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1beta1 "open-cluster-management.io/api/cluster/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	managedClusterSetsTrackingTableName = "managed_cluster_sets_tracking"
	managedClusterSetsTableName         = "managedclustersets"
	managedClusterSetsMsgKey            = "ManagedClusterSets"
	managedClusterSetBindingsTableName  = "managedclustersetbindings"
	managedClusterSetBindingsMsgKey     = "ManagedClusterSetBindings"
)

// AddManagedClusterSetsTrackingDBToTransportSyncer adds managed-cluster-sets-tracking db to transport syncer to the
// manager. The syncer watches an MCS, LH -> MCs table and syncs Objects from two different tables when the first is
// changed, per leaf-hub.
func AddManagedClusterSetsTrackingDBToTransportSyncer(mgr ctrl.Manager, specDB db.SpecDB,
	transportObj transport.Transport, syncInterval time.Duration) error {
	createManagedClusterSetBindingObjFunc := func() metav1.Object { return &clusterv1beta1.ManagedClusterSetBinding{} }
	createManagedClusterSetObjFunc := func() metav1.Object { return &clusterv1beta1.ManagedClusterSet{} }
	lastSyncTimestampPtr := &time.Time{}

	if err := mgr.Add(&genericDBToTransportSyncer{
		log:            ctrl.Log.WithName("managed-cluster-sets-tracking-db-to-transport-syncer"),
		intervalPolicy: intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		syncBundleFunc: func(ctx context.Context) (bool, error) {
			return syncManagedClusterSetResourcesBasedOnTracking(ctx, transportObj, specDB,
				createManagedClusterSetBindingObjFunc, createManagedClusterSetObjFunc, lastSyncTimestampPtr)
		},
	}); err != nil {
		return fmt.Errorf("failed to add managed-cluster-sets-tracking db to transport syncer - %w", err)
	}

	return nil
}

// this function does not attempt to generalize the usage of the tracking info. in this implementation, we attempt
// to demonstrate the capabilities and simplicity of maintaining a tracking-table, and using it to sync resources
// to leaf-hubs selectively.
// currently, only managed-cluster-sets and managed-cluster-set-bindings are supported. the maintenance/track syncing/
// track reading mechanisms should be re-written when functionality needs to be expanded.
func syncManagedClusterSetResourcesBasedOnTracking(ctx context.Context, transportObj transport.Transport,
	specDB db.SpecDB, createManagedClusterSetBindingObjFunc bundle.CreateObjectFunction,
	createManagedClusterSetObjFunc bundle.CreateObjectFunction, lastSyncTimestampPtr *time.Time) (bool, error) {
	syncManagedClusterSetBindings, syncManagedClusterSets, lastOverallUpdateTimestamp, trackingCheckTimestamp,
		err := checkManagedClusterSetResourcesChanged(ctx, specDB, lastSyncTimestampPtr)
	if err != nil {
		return false, fmt.Errorf("failed to determine if should sync - %w", err)
	}

	if !lastOverallUpdateTimestamp.After(*lastSyncTimestampPtr) { // sync only if something has changed
		return false, nil
	}

	// if we got here, then the last update timestamp from db is after what we have in memory.
	// this means something has changed in db, get updated MCS tracking and sync MCS objects to transport.
	clusterSetToLeafHubsMap, err := specDB.GetUpdatedManagedClusterSetsTracking(ctx,
		managedClusterSetsTrackingTableName, trackingCheckTimestamp)
	if err != nil {
		return false, fmt.Errorf("unable to sync bundle to leaf hubs: failed to get MCS tracking - %w", err)
	}

	// sync LH -> MCS-binding bundles
	if syncManagedClusterSetBindings {
		if err := syncObjectBundlesBasedOnManagedClusterSetTracking(ctx, transportObj, managedClusterSetBindingsMsgKey,
			specDB, managedClusterSetBindingsTableName, bundle.NewBaseObjectsBundle,
			createManagedClusterSetBindingObjFunc, clusterSetToLeafHubsMap); err != nil {
			return false, fmt.Errorf("failed to sync managed-cluster-set-bindings bundles - %w", err)
		}
	}
	// sync LH -> MCS bundles
	if syncManagedClusterSets {
		if err := syncObjectBundlesBasedOnManagedClusterSetTracking(ctx, transportObj, managedClusterSetsMsgKey, specDB,
			managedClusterSetsTableName, bundle.NewBaseObjectsBundle, createManagedClusterSetObjFunc,
			clusterSetToLeafHubsMap); err != nil {
			return false, fmt.Errorf("failed to sync managed-cluster-sets bundles - %w", err)
		}
	}
	// updating value to retain same ptr between calls
	*lastSyncTimestampPtr = *lastOverallUpdateTimestamp

	return true, nil
}

func checkManagedClusterSetResourcesChanged(ctx context.Context, specDB db.SpecDB,
	lastSyncTimestampPtr *time.Time) (bool, bool, *time.Time, *time.Time, error) {
	managedClusterSetBindingsLastUpdateTimestamp, err := specDB.GetLastUpdateTimestamp(ctx,
		managedClusterSetBindingsTableName, true)
	if err != nil {
		return false, false, nil, nil, fmt.Errorf("unable to sync bundle - %w", err)
	}

	managedClusterSetsLastUpdateTimestamp, err := specDB.GetLastUpdateTimestamp(ctx, managedClusterSetsTableName, true)
	if err != nil {
		return false, false, nil, nil, fmt.Errorf("unable to sync bundle - %w", err)
	}

	managedClusterSetTrackingLastUpdateTimestamp, err := specDB.GetLastUpdateTimestamp(ctx,
		managedClusterSetsTrackingTableName, false)
	if err != nil {
		return false, false, nil, nil, fmt.Errorf("unable to sync bundle - %w", err)
	}
	// the time after-which tracking entries are relevant
	trackingCheckTimestamp := lastSyncTimestampPtr
	// if a spec resource table had a change then we should get ALL trackings
	if managedClusterSetBindingsLastUpdateTimestamp.After(*lastSyncTimestampPtr) ||
		managedClusterSetsLastUpdateTimestamp.After(*lastSyncTimestampPtr) {
		trackingCheckTimestamp = &time.Time{}
	}

	clusterSetTrackingChanged := managedClusterSetTrackingLastUpdateTimestamp.After(*lastSyncTimestampPtr)

	syncManagedClusterSetBindings := clusterSetTrackingChanged ||
		managedClusterSetBindingsLastUpdateTimestamp.After(*lastSyncTimestampPtr)

	syncManagedClusterSets := clusterSetTrackingChanged ||
		managedClusterSetsLastUpdateTimestamp.After(*lastSyncTimestampPtr)

	return syncManagedClusterSetBindings, syncManagedClusterSets, getMaxTimestamp([]*time.Time{
		managedClusterSetBindingsLastUpdateTimestamp,
		managedClusterSetsLastUpdateTimestamp,
		managedClusterSetTrackingLastUpdateTimestamp,
	}), trackingCheckTimestamp, nil
}

func syncObjectBundlesBasedOnManagedClusterSetTracking(ctx context.Context, transportObj transport.Transport,
	transportBundleKey string, specDB db.SpecDB, dbTableName string, createBundleFunc bundle.CreateBundleFunction,
	createObjFunc bundle.CreateObjectFunction, clusterSetToLeafHubsMap map[string][]string,
) error {
	// get object bundles mapped by obj name
	mappedManagedClusterSetBundles, lastUpdateTimestamp, err := specDB.GetMappedObjectBundles(ctx, dbTableName,
		createBundleFunc, createObjFunc, extractObjectNameFunc)
	if err != nil {
		return fmt.Errorf("failed to get mapped object bundles from DB - %w", err)
	}

	// build leaf-hub -> object bundles map
	leafHubToObjectsBundleMap, err := buildLeafHubToObjectsBundleMap(bundle.NewBaseObjectsBundle,
		clusterSetToLeafHubsMap, mappedManagedClusterSetBundles)
	if err != nil {
		return fmt.Errorf("unable to sync bundle to leaf hubs - failed to build objects map from DB - %w", err)
	}

	for leafHub, objectsBundle := range leafHubToObjectsBundleMap {
		if err := syncToTransport(transportObj, leafHub, transportBundleKey, lastUpdateTimestamp,
			objectsBundle); err != nil {
			return fmt.Errorf("unable to sync bundle to transport - %w", err)
		}
	}

	return nil
}

func buildLeafHubToObjectsBundleMap(createBundleFunc bundle.CreateBundleFunction,
	clusterSetToLeafHubsMap map[string][]string, mappedObjectBundles map[string]bundle.ObjectsBundle,
) (map[string]bundle.ObjectsBundle, error) {
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
				leafHubToObjectsBundleMap[leafHub] = createBundleFunc()
			}
			// merge content
			if err := mergeObjectBundles(leafHubToObjectsBundleMap[leafHub], objectsBundle); err != nil {
				return nil, fmt.Errorf("failed to merge object bundles : clusterSetName=%s, leafHubName=%s - %w",
					clusterSetName, leafHub, err)
			}
		}
	}

	return leafHubToObjectsBundleMap, nil
}

// mergeObjectBundles merges the content of one ObjectsBundle (source) into another (destination).
func mergeObjectBundles(destination bundle.ObjectsBundle, source bundle.ObjectsBundle) error {
	if destination == source {
		return nil // don't do anything
	}

	destinationObjectsBundle, ok1 := destination.(*bundle.BaseObjectsBundle)
	if !ok1 {
		return errObjectBundleMergeTypeMismatch // shouldn't happen
	}

	sourceObjectsBundle, ok2 := source.(*bundle.BaseObjectsBundle)
	if !ok2 {
		return errObjectBundleMergeTypeMismatch // shouldn't happen
	}

	destinationObjectsBundle.Objects = append(destinationObjectsBundle.Objects, sourceObjectsBundle.Objects...)
	destinationObjectsBundle.DeletedObjects = append(destinationObjectsBundle.DeletedObjects,
		sourceObjectsBundle.DeletedObjects...)

	return nil
}

func extractObjectNameFunc(obj metav1.Object) string {
	return obj.GetName()
}
