package dbsyncer

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
)

type genericDBToTransportSyncer struct {
	log                 logr.Logger
	transport           transport.Transport
	transportBundleKey  string
	lastUpdateTimestamp *time.Time
	intervalPolicy      intervalpolicy.IntervalPolicy
	syncBundleFunc      func(ctx context.Context, transportObj transport.Transport, transportBundleKey string,
		lastUpdateTimestamp *time.Time) (bool, error)
}

func (syncer *genericDBToTransportSyncer) Start(ctx context.Context) error {
	syncer.init(ctx)

	go syncer.periodicSync(ctx)

	<-ctx.Done() // blocking wait for cancel context event
	syncer.log.Info("stopped syncer")

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

	syncer.log.Info("initialized syncer")

	if _, err := syncer.syncBundleFunc(ctx, syncer.transport, syncer.transportBundleKey,
		syncer.lastUpdateTimestamp); err != nil {
		syncer.log.Error(err, "failed to sync bundle")
	}
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

			synced, err := syncer.syncBundleFunc(ctxWithTimeout, syncer.transport, syncer.transportBundleKey,
				syncer.lastUpdateTimestamp)
			if err != nil {
				syncer.log.Error(err, "failed to sync bundle")
			}

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
