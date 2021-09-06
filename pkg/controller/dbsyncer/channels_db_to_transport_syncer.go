package dbsyncer

import (
	"fmt"
	"time"

	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	appsv1 "github.com/open-cluster-management/multicloud-operators-channel/pkg/apis/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	channelsTableName = "channels"
	channelsRuleMessageKey    = "channels"
)

// AddChannelsDBToTransportSyncer adds applications db to transport syncer to the manager.
func AddChannelsDBToTransportSyncer(mgr ctrl.Manager, db db.HubOfHubsSpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	if err := mgr.Add(&genericDBToTransportSyncer{
		log:                ctrl.Log.WithName("channel-db-to-transport-syncer"),
		db:                 db,
		dbTableName:        channelsTableName,
		transport:          transport,
		transportBundleKey: channelsRuleMessageKey,
		syncInterval:       syncInterval,
		createObjFunc:      func() metav1.Object { return &appsv1.Channel{} },
		createBundleFunc:   bundle.NewBaseBundle,
	}); err != nil {
		return fmt.Errorf("failed to add db to transport syncer - %w", err)
	}

	return nil
}
