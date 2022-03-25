package dbsyncer

import (
	"fmt"
	"time"

	clusterv1alpha1 "github.com/open-cluster-management/api/cluster/v1alpha1"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const placementsTableName = "placements"

// AddPlacementsDBToTransportSyncer adds placement rules db to transport syncer to the manager.
func AddPlacementsDBToTransportSyncer(mgr ctrl.Manager, db db.SpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	dbToTransportSyncer := &genericObjectsDBToTransportSyncer{
		genericDBToTransportSyncer: &genericDBToTransportSyncer{
			log:                ctrl.Log.WithName("placements-db-to-transport-syncer"),
			db:                 db,
			dbTableName:        placementsTableName,
			transport:          transport,
			transportBundleKey: datatypes.PlacementsMsgKey,
			intervalPolicy:     intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		},
		createObjFunc:    func() metav1.Object { return &clusterv1alpha1.Placement{} },
		createBundleFunc: bundle.NewBaseBundle,
	}

	dbToTransportSyncer.syncBundleFunc = dbToTransportSyncer.syncObjectsBundle

	if err := mgr.Add(dbToTransportSyncer); err != nil {
		return fmt.Errorf("failed to add placement db to transport syncer - %w", err)
	}

	return nil
}
