package dbsyncer

import (
	"fmt"
	"time"

	configv1 "github.com/open-cluster-management/hub-of-hubs-data-types/apis/config/v1"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	configTableName = "configs"
	configsMsgKey   = "Configs"
)

// AddHoHConfigDBToTransportSyncer adds hub-of-hubs config db to transport syncer to the manager.
func AddHoHConfigDBToTransportSyncer(mgr ctrl.Manager, db db.HubOfHubsSpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	if err := mgr.Add(&genericDBToTransportSyncer{
		log:                ctrl.Log.WithName("hoh-config-db-to-transport-syncer"),
		db:                 db,
		dbTableName:        configTableName,
		transport:          transport,
		transportBundleKey: configsMsgKey,
		createObjFunc:      func() metav1.Object { return &configv1.Config{} },
		createBundleFunc:   bundle.NewBaseBundle,
		intervalPolicy:     newDefaultSyncerIntervalPolicy(syncInterval),
	}); err != nil {
		return fmt.Errorf("failed to add db to transport syncer - %w", err)
	}

	return nil
}
