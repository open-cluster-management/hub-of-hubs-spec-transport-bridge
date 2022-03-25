package controller

import (
	"fmt"
	"time"

	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/controller/dbsyncer"
	statuswatcher "github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/controller/status-watcher"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

// AddDBToTransportSyncers adds the controllers that send info from DB to transport layer to the Manager.
func AddDBToTransportSyncers(mgr ctrl.Manager, specDB db.SpecDB, transportObj transport.Transport,
	syncInterval time.Duration,
) error {
	addDBSyncerFunctions := []func(ctrl.Manager, db.SpecDB, transport.Transport, time.Duration) error{
		dbsyncer.AddHoHConfigDBToTransportSyncer,
		dbsyncer.AddPoliciesDBToTransportSyncer,
		dbsyncer.AddPlacementRulesDBToTransportSyncer,
		dbsyncer.AddPlacementBindingsDBToTransportSyncer,
		dbsyncer.AddManagedClusterLabelsDBToTransportSyncer,
		dbsyncer.AddApplicationsDBToTransportSyncer,
		dbsyncer.AddChannelsDBToTransportSyncer,
		dbsyncer.AddSubscriptionsDBToTransportSyncer,
	}
	for _, addDBSyncerFunction := range addDBSyncerFunctions {
		if err := addDBSyncerFunction(mgr, specDB, transportObj, syncInterval); err != nil {
			return fmt.Errorf("failed to add DB Syncer: %w", err)
		}
	}

	return nil
}

// AddStatusDBWatchers adds the controllers that watch the status DB to update the spec DB to the Manager.
func AddStatusDBWatchers(mgr ctrl.Manager, specDB db.SpecDB, statusDB db.StatusDB, syncInterval time.Duration) error {
	addStatusDBWatcherFunctions := []func(ctrl.Manager, db.SpecDB, db.StatusDB, time.Duration) error{
		statuswatcher.AddManagedClusterLabelsStatusWatcher,
	}

	for _, addStatusDBWatcherFunction := range addStatusDBWatcherFunctions {
		if err := addStatusDBWatcherFunction(mgr, specDB, statusDB, syncInterval); err != nil {
			return fmt.Errorf("failed to add status watcher: %w", err)
		}
	}

	return nil
}
