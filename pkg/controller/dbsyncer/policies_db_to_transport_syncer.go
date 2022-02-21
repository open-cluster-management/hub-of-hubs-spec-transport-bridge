package dbsyncer

import (
	"fmt"
	"time"

	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/api/v1"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const policiesTableName = "policies"

// AddPoliciesDBToTransportSyncer adds policies db to transport syncer to the manager.
func AddPoliciesDBToTransportSyncer(mgr ctrl.Manager, db db.SpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	dbToTransportSyncer := &genericObjectsDBToTransportSyncer{
		genericDBToTransportSyncer: &genericDBToTransportSyncer{
			log:                ctrl.Log.WithName("policy-db-to-transport-syncer"),
			db:                 db,
			dbTableName:        policiesTableName,
			transport:          transport,
			transportBundleKey: datatypes.PoliciesMsgKey,
			intervalPolicy:     intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		},
		createObjFunc:    func() metav1.Object { return &policiesv1.Policy{} },
		createBundleFunc: bundle.NewBaseBundle,
	}

	dbToTransportSyncer.syncBundleFunc = dbToTransportSyncer.syncObjectsBundle

	if err := mgr.Add(dbToTransportSyncer); err != nil {
		return fmt.Errorf("failed to add policies db to transport syncer - %w", err)
	}

	return nil
}
