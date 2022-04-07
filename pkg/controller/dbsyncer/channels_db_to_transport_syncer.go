package dbsyncer

import (
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
func AddChannelsDBToTransportSyncer(mgr ctrl.Manager, db db.SpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	dbToTransportSyncer := &genericObjectsDBToTransportSyncer{
		genericDBToTransportSyncer: &genericDBToTransportSyncer{
			log:                ctrl.Log.WithName("channels-db-to-transport-syncer"),
			db:                 db,
			dbTableName:        channelsTableName,
			transport:          transport,
			transportBundleKey: channelsMsgKey,
			intervalPolicy:     intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		},
		createObjFunc:    func() metav1.Object { return &channelsv1.Channel{} },
		createBundleFunc: bundle.NewBaseBundle,
	}

	dbToTransportSyncer.syncBundleFunc = dbToTransportSyncer.syncObjectsBundle

	if err := mgr.Add(dbToTransportSyncer); err != nil {
		return fmt.Errorf("failed to add channels db to transport syncer - %w", err)
	}

	return nil
}
