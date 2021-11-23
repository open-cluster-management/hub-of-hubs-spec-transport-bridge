package dbsyncer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
)

const (
	timeFormat = "2006-01-02_15-04-05.000000"
)

type genericDBToTransportSyncer struct {
	log                 logr.Logger
	db                  db.SpecDB
	dbTableName         string
	transport           transport.Transport
	transportBundleKey  string
	lastUpdateTimestamp *time.Time
	createObjFunc       bundle.CreateObjectFunction
	createBundleFunc    bundle.CreateBundleFunction
	intervalPolicy      intervalpolicy.IntervalPolicy
}

func (syncer *genericDBToTransportSyncer) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	syncer.init(ctx)

	go syncer.periodicSync(ctx)

	<-stopChannel // blocking wait for stop event
	syncer.log.Info("stopped syncer", "table", syncer.dbTableName)

	return nil
}

func (syncer *genericDBToTransportSyncer) init(ctx context.Context) {
	// on initialization, we initialize the lastUpdateTimestamp from the transport layer, as this is the last timestamp
	// that transport bridge sent an update.
	// later, in SyncBundle, it will check the db if there are newer updates and if yes it will send it with
	// transport layer and update the lastUpdateTimestamp field accordingly.
	timestamp := syncer.initLastUpdateTimestampFromTransport()

	if timestamp != nil {
		syncer.lastUpdateTimestamp = timestamp
	} else {
		syncer.lastUpdateTimestamp = &time.Time{}
	}

	syncer.log.Info("initialized syncer", "table", fmt.Sprintf("spec.%s", syncer.dbTableName))
	syncer.syncBundle(ctx)
}

func (syncer *genericDBToTransportSyncer) initLastUpdateTimestampFromTransport() *time.Time {
	version := syncer.transport.GetVersion(syncer.transportBundleKey, datatypes.SpecBundle)
	if version == "" {
		return nil
	}

	timestamp, err := time.Parse(timeFormat, version)
	if err != nil {
		return nil
	}

	return &timestamp
}

// syncBundle performs the actual sync logic and returns true if bundle was committed to transport, otherwise false.
func (syncer *genericDBToTransportSyncer) syncBundle(ctx context.Context) bool {
	lastUpdateTimestamp, err := syncer.db.GetLastUpdateTimestamp(ctx, syncer.dbTableName)
	if err != nil {
		syncer.log.Error(err, "unable to sync bundle to leaf hubs", syncer.dbTableName)

		return false
	}

	if !lastUpdateTimestamp.After(*syncer.lastUpdateTimestamp) { // sync only if something has changed
		return false
	}

	// if we got here, then the last update timestamp from db is after what we have in memory.
	// this means something has changed in db, syncing to transport.
	bundleResult := syncer.createBundleFunc()
	lastUpdateTimestamp, err = syncer.db.GetBundle(ctx, syncer.dbTableName, syncer.createObjFunc, bundleResult)

	if err != nil {
		syncer.log.Error(err, "unable to sync bundle to leaf hubs", syncer.dbTableName)

		return false
	}

	syncer.lastUpdateTimestamp = lastUpdateTimestamp
	syncer.syncToTransport(syncer.transportBundleKey, datatypes.SpecBundle, lastUpdateTimestamp, bundleResult)

	return true
}

func (syncer *genericDBToTransportSyncer) periodicSync(ctx context.Context) {
	ticker := time.NewTicker(syncer.intervalPolicy.GetInterval())

	for {
		select {
		case <-ctx.Done(): // we have received a signal to stop
			ticker.Stop()
			return

		case <-ticker.C:
			// define timeout of max sync interval on the sync function
			ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, syncer.intervalPolicy.GetMaxInterval())
			synced := syncer.syncBundle(ctxWithTimeout)

			cancelFunc() // cancel child ctx and is used to cleanup resources once context expires or sync is done.

			// get current sync interval
			currentInterval := syncer.intervalPolicy.GetInterval()

			// notify policy whether sync was actually performed or skipped
			if synced {
				syncer.intervalPolicy.Evaluate()
			} else {
				syncer.intervalPolicy.Reset()
			}

			// get reevaluated sync interval
			reevaluatedInterval := syncer.intervalPolicy.GetInterval()

			// reset ticker if needed
			if currentInterval != reevaluatedInterval {
				ticker.Reset(reevaluatedInterval)
				syncer.log.Info(fmt.Sprintf("sync interval has been reset to %s", reevaluatedInterval.String()))
			}
		}
	}
}

func (syncer *genericDBToTransportSyncer) syncToTransport(id string, objType string, timestamp *time.Time,
	payload bundle.Bundle) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		syncer.log.Error(err, "failed to sync object", fmt.Sprintf("object type %s with id %s", objType, id))
		return
	}

	syncer.transport.SendAsync(id, objType, timestamp.Format(timeFormat), payloadBytes)
}
