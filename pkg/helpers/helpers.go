package helpers

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
)

const timeFormat = "2006-01-02_15-04-05.000000"

// SyncObjectsToTransport syncs an objects bundle to transport.
func SyncObjectsToTransport(transport transport.Transport, destination string, objID string,
	objType string, timestamp *time.Time, payload bundle.ObjectsBundle) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to sync to transport - %w", err)
	}

	transport.SendAsync(destination, objID, objType, timestamp.Format(timeFormat), payloadBytes)

	return nil
}
