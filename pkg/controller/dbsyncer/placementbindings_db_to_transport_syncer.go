package dbsyncer

import (
	"context"
	"fmt"
	"time"

	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/api/v1"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	placementBindingsTableName = "placementbindings"
	placementBindingsMsgKey    = "PlacementBindings"
)

// AddPlacementBindingsDBToTransportSyncer adds placement bindings db to transport syncer to the manager.
func AddPlacementBindingsDBToTransportSyncer(mgr ctrl.Manager, specDB db.SpecDB, transportObj transport.Transport,
	syncInterval time.Duration) error {
	createObjFunc := func() metav1.Object { return &policiesv1.PlacementBinding{} }
	lastSyncTimestampPtr := &time.Time{}

	if err := mgr.Add(&genericDBToTransportSyncer{
		log:            ctrl.Log.WithName("placement-bindings-db-to-transport-syncer"),
		intervalPolicy: intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		syncBundleFunc: func(ctx context.Context) (bool, error) {
			return syncObjectsBundle(ctx, transportObj, placementBindingsMsgKey, specDB, placementBindingsTableName,
				createObjFunc, bundle.NewBaseBundle, lastSyncTimestampPtr)
		},
	}); err != nil {
		return fmt.Errorf("failed to add placement bindings db to transport syncer - %w", err)
	}

	return nil
}
