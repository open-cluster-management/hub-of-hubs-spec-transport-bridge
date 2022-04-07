package dbsyncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const timeFormat = "2006-01-02_15-04-05.000000"

var errObjectBundleMergeTypeMismatch = errors.New("ObjectBundle type mismatch, should be a BaseObjectsBundle")

// syncObjectsBundle performs the actual sync logic and returns true if bundle was committed to transport,
// otherwise false.
func syncObjectsBundle(ctx context.Context, transportObj transport.Transport, transportBundleKey string,
	specDB db.SpecDB, dbTableName string, createObjFunc bundle.CreateObjectFunction,
	createBundleFunc bundle.CreateBundleFunction, lastSyncTimestampPtr *time.Time) (bool, error) {
	lastUpdateTimestamp, err := specDB.GetLastUpdateTimestamp(ctx, dbTableName, true) // filter local resources
	if err != nil {
		return false, fmt.Errorf("unable to sync bundle - %w", err)
	}

	if !lastUpdateTimestamp.After(*lastSyncTimestampPtr) { // sync only if something has changed
		return false, nil
	}

	// if we got here, then the last update timestamp from db is after what we have in memory.
	// this means something has changed in db, syncing to transport.
	bundleResult := createBundleFunc()
	lastUpdateTimestamp, err = specDB.GetObjectsBundle(ctx, dbTableName, createObjFunc, bundleResult)

	if err != nil {
		return false, fmt.Errorf("unable to sync bundle - %w", err)
	}

	if err := syncToTransport(transportObj, transport.Broadcast, transportBundleKey, lastUpdateTimestamp,
		bundleResult); err != nil {
		return false, fmt.Errorf("unable to sync bundle to transport - %w", err)
	}

	// updating value to retain same ptr between calls
	*lastSyncTimestampPtr = *lastUpdateTimestamp

	return true, nil
}

// syncToTransport syncs an objects bundle to transport.
func syncToTransport(transportObj transport.Transport, destination string, objID string,
	timestamp *time.Time, payload interface{}) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to sync {objID: %s, destination: %s} to transport - %w", objID, destination, err)
	}

	transportObj.SendAsync(destination, objID, datatypes.SpecBundle, timestamp.Format(timeFormat), payloadBytes)

	return nil
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
