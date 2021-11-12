package dbsyncer

import (
	"fmt"
	"time"

	appsv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/apps/v1"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	placementRulesTableName = "placementrules"
	placementRulesMsgKey    = "PlacementRules"
)

// AddPlacementRulesDBToTransportSyncer adds placement rules db to transport syncer to the manager.
func AddPlacementRulesDBToTransportSyncer(mgr ctrl.Manager, db db.HubOfHubsSpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	if err := mgr.Add(&genericDBToTransportSyncer{
		log:                ctrl.Log.WithName("placement-rules-db-to-transport-syncer"),
		db:                 db,
		dbTableName:        placementRulesTableName,
		transport:          transport,
		transportBundleKey: placementRulesMsgKey,
		createObjFunc:      func() metav1.Object { return &appsv1.PlacementRule{} },
		createBundleFunc:   bundle.NewBaseBundle,
		intervalPolicy:     intervalpolicy.NewExponentialBackoffIntervalPolicy(syncInterval),
	}); err != nil {
		return fmt.Errorf("failed to add db to transport syncer - %w", err)
	}

	return nil
}
