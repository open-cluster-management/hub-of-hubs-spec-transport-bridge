package dbsyncer

import (
	"fmt"
	"time"

	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	policiesTableName = "policies"
	policiesMsgKey    = "Policies"
)

// AddPoliciesDBToTransportSyncer adds policies db to transport syncer to the manager.
func AddPoliciesDBToTransportSyncer(mgr ctrl.Manager, db db.HubOfHubsSpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	if err := mgr.Add(&genericDBToTransportSyncer{
		log:                ctrl.Log.WithName("policy-db-to-transport-syncer"),
		db:                 db,
		dbTableName:        policiesTableName,
		transport:          transport,
		transportBundleKey: policiesMsgKey,
		createObjFunc:      func() metav1.Object { return &policiesv1.Policy{} },
		createBundleFunc:   bundle.NewBaseBundle,
		intervalPolicy:     intervalpolicy.NewExponentialBackoffIntervalPolicy(syncInterval),
	}); err != nil {
		return fmt.Errorf("failed to add db to transport syncer - %w", err)
	}

	return nil
}
