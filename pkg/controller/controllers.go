package controller

import (
	"fmt"
	"time"

	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/controller/dbsyncer"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

// AddDBToTransportSyncers adds the controllers that send info from DB to transport layer to the Manager.
func AddDBToTransportSyncers(mgr ctrl.Manager, specDB db.HubOfHubsSpecDB, specTransport transport.Transport,
	syncInterval time.Duration) error {
	addDBSyncerFunctions := []func(ctrl.Manager, db.HubOfHubsSpecDB, transport.Transport,
		time.Duration) error{
		dbsyncer.AddHoHConfigDBToTransportSyncer,
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
