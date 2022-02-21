package dbsyncer

import (
	"context"
	"encoding/json"
	"time"

	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
)

type genericObjectsDBToTransportSyncer struct {
	*genericDBToTransportSyncer
	createObjFunc    bundle.CreateObjectFunction
	createBundleFunc bundle.CreateBundleFunction
}

// syncObjectsBundle performs the actual sync logic and returns true if bundle was committed to transport,
// otherwise false.
func (syncer *genericObjectsDBToTransportSyncer) syncObjectsBundle(ctx context.Context) bool {
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
	bundleResult := syncer.createBundleFunc()
	lastUpdateTimestamp, err = syncer.db.GetObjectsBundle(ctx, syncer.dbTableName, syncer.createObjFunc, bundleResult)

	if err != nil {
		syncer.log.Error(err, "unable to sync bundle to leaf hubs", "tableName", syncer.dbTableName)

		return false
	}

	syncer.lastUpdateTimestamp = lastUpdateTimestamp
	syncer.syncToTransport(syncer.transportBundleKey, datatypes.SpecBundle, lastUpdateTimestamp, bundleResult)

	return true
}

func (syncer *genericObjectsDBToTransportSyncer) syncToTransport(objID string, objType string, timestamp *time.Time,
	payload bundle.ObjectsBundle) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		syncer.log.Error(err, "failed to sync object", "objectId", objID, "objectType", objType)
		return
	}

	syncer.transport.SendAsync("", objID, objType, timestamp.Format(timeFormat), payloadBytes)
}
