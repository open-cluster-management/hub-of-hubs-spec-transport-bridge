package dbsyncer

import (
	"fmt"
	"time"

	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	placementBindingsTableName = "placementbindings"
	placementBindingsMsgKey    = "PlacementBindings"
)

// AddPlacementBindingsDBToTransportSyncer adds placement bindings db to transport syncer to the manager.
func AddPlacementBindingsDBToTransportSyncer(mgr ctrl.Manager, db db.HubOfHubsSpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	if err := mgr.Add(&genericDBToTransportSyncer{
		log:                ctrl.Log.WithName("policy-db-to-transport-syncer"),
		db:                 db,
		dbTableName:        placementBindingsTableName,
		transport:          transport,
		transportBundleKey: placementBindingsMsgKey,
		syncInterval:       syncInterval,
		createObjFunc:      func() metav1.Object { return &policiesv1.PlacementBinding{} },
		createBundleFunc:   bundle.NewBaseBundle,
	}); err != nil {
		return fmt.Errorf("failed to add db to transport syncer - %w", err)
	}

	return nil
}
