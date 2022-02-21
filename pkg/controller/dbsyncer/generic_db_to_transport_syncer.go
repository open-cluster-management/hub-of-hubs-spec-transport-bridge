package dbsyncer

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
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
	syncBundleFunc      func(ctx context.Context) bool
	intervalPolicy      intervalpolicy.IntervalPolicy
}

func (syncer *genericDBToTransportSyncer) Start(ctx context.Context) error {
	syncer.init(ctx)

	go syncer.periodicSync(ctx)

	<-ctx.Done() // blocking wait for cancel context event
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
	syncer.syncBundleFunc(ctx)
}

func (syncer *genericDBToTransportSyncer) initLastUpdateTimestampFromTransport() *time.Time {
	version := syncer.transport.GetVersion(syncer.transportBundleKey, datatypes.SpecBundle)
	if version == "" {
		return &time.Time{}
	}

	timestamp, err := time.Parse(timeFormat, version)
	if err != nil {
		return &time.Time{}
	}

	return &timestamp
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
			synced := syncer.syncBundleFunc(ctxWithTimeout)

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
