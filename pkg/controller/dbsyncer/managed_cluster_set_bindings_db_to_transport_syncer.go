package dbsyncer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clusterv1alpha1 "github.com/open-cluster-management/api/cluster/v1alpha1"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const managedClusterSetBindingsTableName = "managedclustersetbindings"

// AddManagedClusterSetBindingsDBToTransportSyncer adds managed-cluster-set-bindings db to transport syncer to the
// manager.
func AddManagedClusterSetBindingsDBToTransportSyncer(mgr ctrl.Manager, db db.SpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	dbToTransportSyncer := &managedClusterSetBindingsDBToTransportSyncer{
		genericDBToTransportSyncer: &genericDBToTransportSyncer{
			log:                ctrl.Log.WithName("managed-cluster-set-bindings-db-to-transport-syncer"),
			db:                 db,
			dbTableName:        managedClusterSetsTrackingTableName, // reconcile by tracking table
			transport:          transport,
			transportBundleKey: datatypes.ManagedClusterSetBindingsMsgKey,
			intervalPolicy:     intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		},
		createObjFunc:    func() metav1.Object { return &clusterv1alpha1.ManagedClusterSetBinding{} },
		createBundleFunc: bundle.NewBaseBundle,
	}

	dbToTransportSyncer.syncBundleFunc = dbToTransportSyncer.syncManagedClusterSetBindingsBundles

	if err := mgr.Add(dbToTransportSyncer); err != nil {
		return fmt.Errorf("failed to add managed-cluster labels db to transport syncer - %w", err)
	}

	return nil
}

type managedClusterSetBindingsDBToTransportSyncer struct {
	*genericDBToTransportSyncer
	createObjFunc    bundle.CreateObjectFunction
	createBundleFunc bundle.CreateBundleFunction
}

// syncManagedClusterSetBindingsBundles is invoked whenever a tracking entry is changed. Once a change is found,
// the affected ManagedClusterSetBindings are read from the resource's spec table and shipped out as an objects bundle.
func (syncer *managedClusterSetBindingsDBToTransportSyncer) syncManagedClusterSetBindingsBundles(ctx context.Context,
) bool {
	lastUpdateTimestamp, err := syncer.db.GetLastUpdateTimestamp(ctx, syncer.dbTableName)
	if err != nil {
		syncer.log.Error(err, "unable to sync bundle to leaf hubs", "tableName", syncer.dbTableName)

		return false
	}

	if !lastUpdateTimestamp.After(*syncer.lastUpdateTimestamp) { // sync only if something has changed
		return false
	}

	// get MCS-binding bundles mapped by MCS name
	mappedManagedClusterSetBindingBundles, _, err := syncer.db.GetMappedObjectBundles(ctx,
		managedClusterSetBindingsTableName, syncer.createBundleFunc,
		syncer.createObjFunc, func(obj metav1.Object) string {
			return obj.GetName()
		})
	if err != nil {
		syncer.log.Error(err, "failed to get mapped object bundles from DB",
			"tableName", managedClusterSetBindingsTableName)

		return false
	}

	// if there are no bindings, don't proceed
	if len(mappedManagedClusterSetBindingBundles) == 0 {
		return false
	}

	// if we got here, then the last update timestamp from db is after what we have in memory.
	// this means something has changed in db, get updated MCS tracking and sync MCS-binding objects to transport.
	clusterSetToLeafHubsMap, lastUpdateTimestamp,
		err := syncer.db.GetUpdatedManagedClusterSetsTracking(ctx, syncer.dbTableName, syncer.lastUpdateTimestamp)
	if err != nil {
		syncer.log.Error(err, "unable to sync bundle to leaf hubs", "tableName", syncer.dbTableName)

		return false
	}

	// build leaf-hub -> MCS-binding bundles map
	leafHubToObjectsBundleMap, err := syncer.buildLeafHubToObjectsBundleMap(ctx, clusterSetToLeafHubsMap,
		mappedManagedClusterSetBindingBundles)
	if err != nil {
		syncer.log.Error(err, "unable to sync bundle to leaf hubs", "tableName", syncer.dbTableName)

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

func (syncer *managedClusterSetBindingsDBToTransportSyncer) syncToTransport(destination string, objID string,
	objType string, timestamp *time.Time, payload bundle.ObjectsBundle) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		syncer.log.Error(err, "failed to sync object", "objectId", objID, "objectType", objType)
		return
	}

	syncer.transport.SendAsync(destination, objID, objType, timestamp.Format(timeFormat), payloadBytes)
}

func (syncer *managedClusterSetBindingsDBToTransportSyncer) buildLeafHubToObjectsBundleMap(_ context.Context,
	clusterSetToLeafHubsMap map[string][]string,
	mappedManagedClusterSetBindingBundles map[string]bundle.ObjectsBundle) (map[string]bundle.ObjectsBundle, error) {
	leafHubToObjectsBundleMap := map[string]bundle.ObjectsBundle{}
	// build lh -> MCS bundle
	for clusterSetName, LeafHubs := range clusterSetToLeafHubsMap {
		objectsBundle, found := mappedManagedClusterSetBindingBundles[clusterSetName]
		if !found {
			return nil, errManagedClusterSetTrackingFoundButCRIsNot
		}
		// add objects bundle for all leaf hubs in mapping
		for _, leafHub := range LeafHubs {
			// create mapping if it doesn't exist
			if _, found := leafHubToObjectsBundleMap[leafHub]; !found {
				leafHubToObjectsBundleMap[leafHub] = syncer.createBundleFunc()
			}
			// merge content
			if err := leafHubToObjectsBundleMap[leafHub].MergeBundle(objectsBundle); err != nil {
				syncer.log.Error(err, "failed to merge ManagedClusterSet bundles", "clusterSetName", clusterSetName,
					"leafHubName", leafHub)
			}
		}
	}

	return leafHubToObjectsBundleMap, nil
}
