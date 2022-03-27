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

const managedClusterSetsTableName = "managedclustersets"

// AddManagedClusterSetsDBToTransportSyncer adds managed-cluster-sets db to transport syncer to the manager.
func AddManagedClusterSetsDBToTransportSyncer(mgr ctrl.Manager, db db.SpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	dbToTransportSyncer := &managedClusterSetsDBToTransportSyncer{
		genericDBToTransportSyncer: &genericDBToTransportSyncer{
			log:                ctrl.Log.WithName("managed-cluster-sets-db-to-transport-syncer"),
			db:                 db,
			dbTableName:        managedClusterSetsTableName,
			transport:          transport,
			transportBundleKey: datatypes.ManagedClusterSetsMsgKey,
			intervalPolicy:     intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		},
		createObjFunc:    func() metav1.Object { return &v1beta1.ManagedClusterSet{} },
		createBundleFunc: bundle.NewBaseBundle,
	}

	dbToTransportSyncer.syncBundleFunc = dbToTransportSyncer.syncManagedClusterSetsBundles

	if err := mgr.Add(dbToTransportSyncer); err != nil {
		return fmt.Errorf("failed to add managed-cluster labels db to transport syncer - %w", err)
	}

	return nil
}

type managedClusterSetsDBToTransportSyncer struct {
	*genericDBToTransportSyncer
	createObjFunc    bundle.CreateObjectFunction
	createBundleFunc bundle.CreateBundleFunction
}

func (syncer *managedClusterSetsDBToTransportSyncer) syncManagedClusterSetsBundles(ctx context.Context) bool {
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
	// this means something has changed in db, sync all per LH.
	mappedManagedClusterSetBundles, _, err := syncer.db.GetMappedObjectBundles(ctx, syncer.dbTableName,
		syncer.createBundleFunc, syncer.createObjFunc, func(obj metav1.Object) string {
			return obj.GetName()
		})
	if err != nil {
		syncer.log.Error(err, "failed to get mapped object bundles from DB",
			"tableName", syncer.dbTableName)

		return false
	}

	return syncer.syncToLeafHubs(ctx, mappedManagedClusterSetBundles, lastUpdateTimestamp)
}

func (syncer *managedClusterSetsDBToTransportSyncer) syncToLeafHubs(ctx context.Context,
	mappedObjectBundles map[string]bundle.ObjectsBundle, lastUpdateTimestamp *time.Time) bool {
	// get ALL cluster-set -> leaf-hubs tracking
	clusterSetToLeafHubsMap, _,
		err := syncer.db.GetUpdatedManagedClusterSetsTracking(ctx, managedClusterSetsTrackingTableName, &time.Time{})
	if err != nil {
		syncer.log.Error(err, "unable to sync bundle to leaf hubs - failed to get MCS tracking",
			"tableName", syncer.dbTableName)

		return false
	}

	// build leaf-hub -> MCS bundles map
	leafHubToObjectsBundleMap, err := syncer.buildLeafHubToObjectsBundleMap(ctx, clusterSetToLeafHubsMap,
		mappedObjectBundles)
	if err != nil {
		syncer.log.Error(err, "unable to sync bundle to leaf hubs - failed to build objects map",
			"tableName", syncer.dbTableName)

		return false
	}

	// send only objects that correspond to a tracking
	syncer.lastUpdateTimestamp = lastUpdateTimestamp

	// sync bundle per leaf hub
	for leafHubName, objectsBundle := range leafHubToObjectsBundleMap {
		syncer.syncToTransport(leafHubName, syncer.transportBundleKey, datatypes.SpecBundle, lastUpdateTimestamp,
			objectsBundle)
	}

	return true
}

func (syncer *managedClusterSetsDBToTransportSyncer) buildLeafHubToObjectsBundleMap(_ context.Context,
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
				return nil, fmt.Errorf("failed to merge object bundles : clusterSetName {%s}, leafHubName {%s} - %w",
					clusterSetName, leafHub, err)
			}
		}
	}

	return leafHubToObjectsBundleMap, nil
}

func (syncer *managedClusterSetsDBToTransportSyncer) syncToTransport(destination string, objID string,
	objType string, timestamp *time.Time, payload bundle.ObjectsBundle) {
	if err := helpers.SyncObjectsToTransport(syncer.transport, destination, objID, objType, timestamp,
		payload); err != nil {
		syncer.log.Error(err, "failed to sync object", "objectId", objID, "objectType", objType)
	}
}
