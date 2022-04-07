package dbsyncer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
)

const timeFormat = "2006-01-02_15-04-05.000000"

// syncObjectsBundle performs the actual sync logic and returns true if bundle was committed to transport,
// otherwise false.
func syncObjectsBundle(ctx context.Context, transportObj transport.Transport, transportBundleKey string,
	specDB db.SpecDB, dbTableName string, createObjFunc bundle.CreateObjectFunction,
	createBundleFunc bundle.CreateBundleFunction, lastSyncTimestampPtr *time.Time) (bool, error) {
	lastUpdateTimestamp, err := specDB.GetLastUpdateTimestamp(ctx, dbTableName)
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

	*lastSyncTimestampPtr = *lastUpdateTimestamp

	if err := syncToTransport(transportObj, transport.Broadcast, transportBundleKey, datatypes.SpecBundle,
		lastUpdateTimestamp, bundleResult); err != nil {
		return false, fmt.Errorf("unable to sync bundle to transport - %w", err)
	}

	return true, nil
}

// syncToTransport syncs an objects bundle to transport.
func syncToTransport(transportObj transport.Transport, destination string, objID string,
	objType string, timestamp *time.Time, payload interface{}) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to sync {objID: %s, destination: %s} to transport - %w", objID, destination, err)
	}

	transportObj.SendAsync(destination, objID, objType, timestamp.Format(timeFormat), payloadBytes)

	return nil
}
