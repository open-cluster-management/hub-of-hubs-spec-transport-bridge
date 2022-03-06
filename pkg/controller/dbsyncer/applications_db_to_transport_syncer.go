package dbsyncer

import (
	"fmt"
	"time"

	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	appsv1 "sigs.k8s.io/application/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	applicationsTableName  = "Applications"
	applicationsMessageKey = "Applications"
)

// AddApplicationsDBToTransportSyncer adds applications db to transport syncer to the manager.
func AddApplicationsDBToTransportSyncer(mgr ctrl.Manager, db db.SpecDB, transport transport.Transport,
	syncInterval time.Duration) error {
	if err := mgr.Add(&genericDBToTransportSyncer{
		log:                ctrl.Log.WithName("applications-db-to-transport-syncer"),
		db:                 db,
		dbTableName:        applicationsTableName,
		transport:          transport,
		transportBundleKey: applicationsMessageKey,
		intervalPolicy:     intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
		createObjFunc:      func() metav1.Object { return &appsv1.Application{} },
		createBundleFunc:   bundle.NewBaseBundle,
	}); err != nil {
		return fmt.Errorf("failed to add db to transport syncer - %w", err)
	}

	return nil
}
