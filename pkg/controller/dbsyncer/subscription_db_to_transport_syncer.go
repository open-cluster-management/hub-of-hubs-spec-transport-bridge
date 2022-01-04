package dbsyncer

import (
	"fmt"
	"time"

	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	appsv1 "github.com/open-cluster-management/multicloud-operators-subscription/pkg/apis/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	subscriptionsTableName     = "subscriptions"
	subscriptionRuleMessageKey = "subscriptions"
)

// AddSubscriptionsDBToTransportSyncer adds subscriptions db to transport syncer to the manager.
func AddSubscriptionsDBToTransportSyncer(mgr ctrl.Manager, db db.SpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	if err := mgr.Add(&genericDBToTransportSyncer{
		log:                ctrl.Log.WithName("subscription-db-to-transport-syncer"),
		db:                 db,
		dbTableName:        subscriptionsTableName,
		transport:          transport,
		transportBundleKey: subscriptionRuleMessageKey,
		intervalPolicy:     intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		createObjFunc:      func() metav1.Object { return &appsv1.Subscription{} },
		createBundleFunc:   bundle.NewBaseBundle,
	}); err != nil {
		return fmt.Errorf("failed to add db to transport syncer - %w", err)
	}

	return nil
}
