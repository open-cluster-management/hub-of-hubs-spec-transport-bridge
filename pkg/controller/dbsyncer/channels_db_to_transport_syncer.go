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
	channelsTableName  = "channels"
	channelsMessageKey = "Channels"
)

// AddChannelsDBToTransportSyncer adds channels db to transport syncer to the manager.
func AddChannelsDBToTransportSyncer(mgr ctrl.Manager, db db.SpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	if err := mgr.Add(&genericDBToTransportSyncer{
		log:                ctrl.Log.WithName("channel-db-to-transport-syncer"),
		db:                 db,
		dbTableName:        channelsTableName,
		transport:          transport,
		transportBundleKey: channelsMessageKey,
		createObjFunc:      func() metav1.Object { return &channelsv1.Channel{} },
		createBundleFunc:   bundle.NewBaseBundle,
		intervalPolicy:     intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
	}); err != nil {
		return fmt.Errorf("failed to add channel db to transport syncer - %w", err)
	}

	return nil
}
