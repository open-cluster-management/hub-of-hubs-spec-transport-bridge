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

func getMaxTimestamp(timestamps []*time.Time) *time.Time {
	max := &time.Time{}

	for _, timestamp := range timestamps {
		if timestamp.After(*max) {
			max = timestamp
		}
	}

	return max
}
