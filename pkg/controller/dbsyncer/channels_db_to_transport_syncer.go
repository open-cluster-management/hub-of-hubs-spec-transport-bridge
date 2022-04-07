package dbsyncer

import (
	"context"
	"fmt"
	"time"

	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	channelsv1 "open-cluster-management.io/multicloud-operators-channel/pkg/apis/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	channelsTableName = "channels"
	channelsMsgKey    = "Channels"
)

// AddChannelsDBToTransportSyncer adds channels db to transport syncer to the manager.
func AddChannelsDBToTransportSyncer(mgr ctrl.Manager, specDB db.SpecDB, transportObj transport.Transport,
	syncInterval time.Duration) error {
	createObjFunc := func() metav1.Object { return &channelsv1.Channel{} }

	if err := mgr.Add(&genericDBToTransportSyncer{
		log:                ctrl.Log.WithName("channels-db-to-transport-syncer"),
		transport:          transportObj,
		transportBundleKey: channelsMsgKey,
		intervalPolicy:     intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		syncBundleFunc: func(ctx context.Context, transportObj transport.Transport, transportBundleKey string,
			lastSyncTimestampPtr *time.Time) (bool, error) {
			return syncObjectsBundle(ctx, transportObj, transportBundleKey, specDB, channelsTableName,
				createObjFunc, bundle.NewBaseBundle, lastSyncTimestampPtr)
		},
	}); err != nil {
		return fmt.Errorf("failed to add channels db to transport syncer - %w", err)
	}

	return nil
}
