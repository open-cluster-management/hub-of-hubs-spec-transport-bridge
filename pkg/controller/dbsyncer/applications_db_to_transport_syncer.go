package dbsyncer

import (
	"context"
	"fmt"
	"time"

	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	appsv1beta1 "sigs.k8s.io/application/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	applicationsTableName = "applications"
	applicationsMsgKey    = "Applications"
)

// AddApplicationsDBToTransportSyncer adds applications db to transport syncer to the manager.
func AddApplicationsDBToTransportSyncer(mgr ctrl.Manager, specDB db.SpecDB, transportObj transport.Transport,
	syncInterval time.Duration) error {
	createObjFunc := func() metav1.Object { return &appsv1beta1.Application{} }

	if err := mgr.Add(&genericDBToTransportSyncer{
		log:                ctrl.Log.WithName("applications-db-to-transport-syncer"),
		transport:          transportObj,
		transportBundleKey: applicationsMsgKey,
		intervalPolicy:     intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		syncBundleFunc: func(ctx context.Context, transportObj transport.Transport, transportBundleKey string,
			lastSyncTimestampPtr *time.Time) (bool, error) {
			return syncObjectsBundle(ctx, transportObj, transportBundleKey, specDB, applicationsTableName,
				createObjFunc, bundle.NewBaseBundle, lastSyncTimestampPtr)
		},
	}); err != nil {
		return fmt.Errorf("failed to add applications db to transport syncer - %w", err)
	}

	return nil
}
