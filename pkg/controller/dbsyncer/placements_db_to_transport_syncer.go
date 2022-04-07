package dbsyncer

import (
	"context"
	"fmt"
	"time"

	clusterv1alpha1 "github.com/open-cluster-management/api/cluster/v1alpha1"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	placementsTableName = "placements"
	placementsMsgKey    = "Placements"
)

// AddPlacementsDBToTransportSyncer adds placement db to transport syncer to the manager.
func AddPlacementsDBToTransportSyncer(mgr ctrl.Manager, specDB db.SpecDB, transportObj transport.Transport,
	syncInterval time.Duration) error {
	createObjFunc := func() metav1.Object { return &clusterv1alpha1.Placement{} }
	lastSyncTimestampPtr := &time.Time{}

	if err := mgr.Add(&genericDBToTransportSyncer{
		log:            ctrl.Log.WithName("placements-db-to-transport-syncer"),
		intervalPolicy: intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		syncBundleFunc: func(ctx context.Context) (bool, error) {
			return syncObjectsBundle(ctx, transportObj, placementsMsgKey, specDB, placementsTableName,
				createObjFunc, bundle.NewBaseObjectsBundle, lastSyncTimestampPtr)
		},
	}); err != nil {
		return fmt.Errorf("failed to add placements db to transport syncer - %w", err)
	}

	return nil
}
