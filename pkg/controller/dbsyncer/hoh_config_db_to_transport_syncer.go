package dbsyncer

import (
	"context"
	"fmt"
	"time"

	configv1 "github.com/stolostron/hub-of-hubs-data-types/apis/config/v1"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	configTableName = "configs"
	configMsgKey    = "Config"
)

// AddHoHConfigDBToTransportSyncer adds hub-of-hubs config db to transport syncer to the manager.
func AddHoHConfigDBToTransportSyncer(mgr ctrl.Manager, specDB db.SpecDB, transportObj transport.Transport,
	syncInterval time.Duration) error {
	createObjFunc := func() metav1.Object { return &configv1.Config{} }

	if err := mgr.Add(&genericDBToTransportSyncer{
		log:                ctrl.Log.WithName("hoh-config-db-to-transport-syncer"),
		transport:          transportObj,
		transportBundleKey: configMsgKey,
		intervalPolicy:     intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		syncBundleFunc: func(ctx context.Context, transportObj transport.Transport, transportBundleKey string,
			lastSyncTimestampPtr *time.Time) (bool, error) {
			return syncObjectsBundle(ctx, transportObj, transportBundleKey, specDB, configTableName,
				createObjFunc, bundle.NewBaseBundle, lastSyncTimestampPtr)
		},
	}); err != nil {
		return fmt.Errorf("failed to add config db to transport syncer - %w", err)
	}

	return nil
}
