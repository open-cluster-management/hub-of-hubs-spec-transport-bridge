package controller

import (
	"fmt"
	"time"

	configv1 "github.com/open-cluster-management/hub-of-hubs-data-types/apis/config/v1"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/controller/dbsyncer"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/controller/specsyncer"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// AddToScheme adds all the resources to be processed to the Scheme.
func AddToScheme(s *runtime.Scheme) error {
	schemeBuilders := []*scheme.Builder{configv1.SchemeBuilder}

	for _, schemeBuilder := range schemeBuilders {
		if err := schemeBuilder.AddToScheme(s); err != nil {
			return fmt.Errorf("failed to add scheme: %w", err)
		}
	}

	return nil
}

// AddDBToTransportSyncers adds the controllers that send info from DB to transport layer to the Manager.
func AddDBToTransportSyncers(mgr ctrl.Manager, specDB db.HubOfHubsSpecDB, specTransport transport.Transport,
	syncInterval time.Duration) error {
	addDBSyncerFunctions := []func(ctrl.Manager, db.HubOfHubsSpecDB, transport.Transport,
		time.Duration) error{
		dbsyncer.AddPoliciesDBToTransportSyncer,
		dbsyncer.AddPlacementRulesDBToTransportSyncer,
		dbsyncer.AddPlacementBindingsDBToTransportSyncer,
	}
	for _, addDBSyncerFunction := range addDBSyncerFunctions {
		if err := addDBSyncerFunction(mgr, specDB, specTransport, syncInterval); err != nil {
			return fmt.Errorf("failed to add DB Syncer: %w", err)
		}
	}

	return nil
}

// AddSpecToTransportSyncers adds the controllers that send CRs to transport layer to the Manager.
func AddSpecToTransportSyncers(mgr ctrl.Manager, specTransport transport.Transport, syncInterval time.Duration) error {
	addSpecSyncerFunctions := []func(ctrl.Manager, transport.Transport, time.Duration) error{
		specsyncer.AddConfigSpecToTransportSyncer,
	}

	for _, addSpecSyncerFunction := range addSpecSyncerFunctions {
		if err := addSpecSyncerFunction(mgr, specTransport, syncInterval); err != nil {
			return fmt.Errorf("failed to add Spec Syncer: %w", err)
		}
	}

	return nil
}
