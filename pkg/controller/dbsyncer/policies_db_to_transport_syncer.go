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
	policiesTableName = "policies"
	policiesMsgKey    = "Policies"
)

// AddPoliciesDBToTransportSyncer adds policies db to transport syncer to the manager.
func AddPoliciesDBToTransportSyncer(mgr ctrl.Manager, specDB db.SpecDB, transportObj transport.Transport,
	syncInterval time.Duration) error {
	createObjFunc := func() metav1.Object { return &policiesv1.Policy{} }

	if err := mgr.Add(&genericDBToTransportSyncer{
		log:                ctrl.Log.WithName("policies-db-to-transport-syncer"),
		transport:          transportObj,
		transportBundleKey: policiesMsgKey,
		intervalPolicy:     intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		syncBundleFunc: func(ctx context.Context, transportObj transport.Transport, transportBundleKey string,
			lastSyncTimestampPtr *time.Time) (bool, error) {
			return syncObjectsBundle(ctx, transportObj, transportBundleKey, specDB, policiesTableName,
				createObjFunc, bundle.NewBaseBundle, lastSyncTimestampPtr)
		},
	}); err != nil {
		return fmt.Errorf("failed to add policies db to transport syncer - %w", err)
	}

	return nil
}
