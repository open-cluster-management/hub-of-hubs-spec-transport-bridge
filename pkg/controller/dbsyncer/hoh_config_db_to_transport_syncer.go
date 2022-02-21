package dbsyncer

import (
	"fmt"
	"time"

	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	configv1 "github.com/stolostron/hub-of-hubs-data-types/apis/config/v1"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	configTableName = "configs"
)

// AddHoHConfigDBToTransportSyncer adds hub-of-hubs config db to transport syncer to the manager.
func AddHoHConfigDBToTransportSyncer(mgr ctrl.Manager, db db.SpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	dbToTransportSyncer := &genericObjectsDBToTransportSyncer{
		genericDBToTransportSyncer: &genericDBToTransportSyncer{
			log:                ctrl.Log.WithName("hoh-config-db-to-transport-syncer"),
			db:                 db,
			dbTableName:        configTableName,
			transport:          transport,
			transportBundleKey: datatypes.Config,
			intervalPolicy:     intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		},
		createObjFunc:    func() metav1.Object { return &configv1.Config{} },
		createBundleFunc: bundle.NewBaseBundle,
	}

	dbToTransportSyncer.syncBundleFunc = dbToTransportSyncer.syncObjectsBundle

	if err := mgr.Add(dbToTransportSyncer); err != nil {
		return fmt.Errorf("failed to add config db to transport syncer - %w", err)
	}

	return nil
}
