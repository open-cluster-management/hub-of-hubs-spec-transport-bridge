package dbsyncer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	hohDb "github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
)

const (
	timeFormat = "2006-01-02_15-04-05.000000"
)

type genericDBToTransportSyncer struct {
	log                 logr.Logger
	db                  hohDb.HubOfHubsSpecDB
	dbTableName         string
	transport           transport.Transport
	transportBundleKey  string
	lastUpdateTimestamp *time.Time
	createObjFunc       bundle.CreateObjectFunction
	createBundleFunc    bundle.CreateBundleFunction
	intervalPolicy      syncerIntervalPolicy
}

func (syncer *genericDBToTransportSyncer) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	syncer.init()

	go syncer.syncBundle(ctx)

	<-stopChannel // blocking wait for stop event
	cancelContext()
	syncer.log.Info("stopped syncer", "table", syncer.dbTableName)

	return nil
}

func (syncer *genericDBToTransportSyncer) init() {
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

func (syncer *genericDBToTransportSyncer) syncBundle(ctx context.Context) {
	currentSyncInterval := syncer.intervalPolicy.getInterval()
	ticker := time.NewTicker(currentSyncInterval)

	for {
		select {
		case <-ctx.Done(): // we have received a signal to stop
			ticker.Stop()
			return

		case <-ticker.C:
			lastUpdateTimestamp, err := syncer.db.GetLastUpdateTimestamp(ctx, syncer.dbTableName)
			if err != nil {
				syncer.adjustInterval(ticker, &currentSyncInterval, false)
				syncer.log.Error(err, "unable to sync bundle to leaf hubs", syncer.dbTableName)

				continue
			}

			if !lastUpdateTimestamp.After(*syncer.lastUpdateTimestamp) { // sync only if something has changed
				syncer.adjustInterval(ticker, &currentSyncInterval, false)

				continue
			}

			// if we got here, then the last update timestamp from db is after what we have in memory.
			// this means something has changed in db, syncing to transport.
			bundleResult := syncer.createBundleFunc()
			lastUpdateTimestamp, err = syncer.db.GetBundle(ctx, syncer.dbTableName, syncer.createObjFunc, bundleResult)

			if err != nil {
				syncer.adjustInterval(ticker, &currentSyncInterval, false)
				syncer.log.Error(err, "unable to sync bundle to leaf hubs", syncer.dbTableName)

				continue
			}

			syncer.lastUpdateTimestamp = lastUpdateTimestamp

			syncer.syncToTransport(syncer.transportBundleKey, datatypes.SpecBundle, lastUpdateTimestamp, bundleResult)
			syncer.adjustInterval(ticker, &currentSyncInterval, true)
		}
	}
}

func (syncer *genericDBToTransportSyncer) adjustInterval(ticker *time.Ticker, currentSyncInterval *time.Duration,
	syncPerformed bool) {
	// notify policy whether sync was actually performed or skipped
	if syncPerformed {
		syncer.intervalPolicy.onSyncPerformed()
	} else {
		syncer.intervalPolicy.onSyncSkipped()
	}

	// get recalculated sync interval
	interval := syncer.intervalPolicy.getInterval()

	// reset ticker if sync interval has changed
	if interval != *currentSyncInterval {
		*currentSyncInterval = interval
		ticker.Reset(*currentSyncInterval)
		syncer.log.Info(fmt.Sprintf("sync interval has been reset to %s", currentSyncInterval.String()))
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
