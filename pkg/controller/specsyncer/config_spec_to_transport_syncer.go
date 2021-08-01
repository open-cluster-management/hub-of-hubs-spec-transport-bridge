package specsyncer

import (
	"time"

	configv1 "github.com/open-cluster-management/hub-of-hubs-data-types/apis/config/v1"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/controller/specsyncer/predicate"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	configName    = "hub-of-hubs-config"
	finalizerName = "hub-of-hubs.open-cluster-management.io/config-cleanup"
)

// AddConfigSpecToTransportSyncer adds config spec to transport syncer to the manager.
func AddConfigSpecToTransportSyncer(mgr ctrl.Manager, transport transport.Transport, syncInterval time.Duration) error {
	specToTransportSyncer := &genericSpecToTransportSyncer{
		client:            mgr.GetClient(),
		log:               ctrl.Log.WithName("config-spec-to-transport-syncer"),
		transport:         transport,
		syncInterval:      syncInterval,
		finalizerName:     finalizerName,
		createObjFunc:     func() object { return &configv1.Config{} },
		manipulateObjFunc: manipulateConfig,
	}
	specToTransportSyncer.init()

	return ctrl.NewControllerManagedBy(mgr).
		For(&configv1.Config{}).
		WithEventFilter(predicate.HoHPredicate). // listen to changes only in HoH-system namespace
		Complete(specToTransportSyncer)
}

func manipulateConfig(config object) object {
	config.SetName(configName)
	return config
}
