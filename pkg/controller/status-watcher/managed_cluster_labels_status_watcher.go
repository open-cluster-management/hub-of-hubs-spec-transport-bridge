package statuswatcher

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/intervalpolicy"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	managedClusterLabelsSpecDBTableName        = "managed_clusters_labels"
	managedClusterLabelsStatusDBTableName      = "managed_clusters"
	deletedLabelKeysTrimmingIntervalMultiplier = 100 // low frequency
)

// AddManagedClusterLabelsStatusWatcher adds managedClusterLabelsStatusWatcher to the manager.
func AddManagedClusterLabelsStatusWatcher(mgr ctrl.Manager, specDB db.SpecDB, statusDB db.StatusDB,
	syncInterval time.Duration) error {
	if err := mgr.Add(&managedClusterLabelsStatusWatcher{
		log:                   ctrl.Log.WithName("managed-cluster-labels-status-watcher"),
		specDB:                specDB,
		statusDB:              statusDB,
		labelsSpecTableName:   managedClusterLabelsSpecDBTableName,
		labelsStatusTableName: managedClusterLabelsStatusDBTableName,
		intervalPolicy:        intervalpolicy.NewExponentialBackoffPolicy(syncInterval),
	}); err != nil {
		return fmt.Errorf("failed to add managed-cluster labels status watcher - %w", err)
	}

	return nil
}

// managedClusterLabelsStatusWatcher watches the status managed-clusters status table to sync and update spec
// table where required (e.g., trim deleted_label_keys).
type managedClusterLabelsStatusWatcher struct {
	log                   logr.Logger
	specDB                db.SpecDB
	statusDB              db.StatusDB
	labelsSpecTableName   string
	labelsStatusTableName string
	intervalPolicy        intervalpolicy.IntervalPolicy
}

func (watcher *managedClusterLabelsStatusWatcher) Start(ctx context.Context) error {
	watcher.init(ctx)

	go watcher.updateDeletedLabelsPeriodically(ctx)

	<-ctx.Done() // blocking wait for cancel context event
	watcher.log.Info("stopped watcher", "spec table", fmt.Sprintf("spec.%s", watcher.labelsSpecTableName),
		"status table", fmt.Sprintf("status.%s", watcher.labelsStatusTableName))

	return nil
}

func (watcher *managedClusterLabelsStatusWatcher) init(ctx context.Context) {
	watcher.log.Info("initialized watcher", "spec table", fmt.Sprintf("spec.%s", watcher.labelsSpecTableName),
		"status table", fmt.Sprintf("status.%s", watcher.labelsStatusTableName))
}

func (watcher *managedClusterLabelsStatusWatcher) updateDeletedLabelsPeriodically(ctx context.Context) {
	hubNameFillTicker := time.NewTicker(watcher.intervalPolicy.GetInterval())
	labelsTrimmerTicker := time.NewTicker(watcher.intervalPolicy.GetInterval() *
		deletedLabelKeysTrimmingIntervalMultiplier)

	for {
		select {
		case <-ctx.Done(): // we have received a signal to stop
			hubNameFillTicker.Stop()
			labelsTrimmerTicker.Stop()

			return

		case <-hubNameFillTicker.C:
			// define timeout of max execution interval on the update function
			ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, watcher.intervalPolicy.GetMaxInterval())
			updated := watcher.fillMissingLeafHubNames(ctxWithTimeout)

			cancelFunc() // cancel child ctx and is used to cleanup resources once context expires or update is done.

			// get current update interval
			currentInterval := watcher.intervalPolicy.GetInterval()

			// notify policy whether sync was actually performed or skipped
			if updated {
				watcher.intervalPolicy.Evaluate()
			} else {
				watcher.intervalPolicy.Reset()
			}

			// get reevaluated update interval
			reevaluatedInterval := watcher.intervalPolicy.GetInterval()

			// reset ticker if needed
			if currentInterval != reevaluatedInterval {
				hubNameFillTicker.Reset(reevaluatedInterval)
				watcher.log.Info(fmt.Sprintf("update interval has been reset to %s", reevaluatedInterval.String()))
			}

		case <-labelsTrimmerTicker.C:
			// define timeout of max execution interval on the update function
			ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, watcher.intervalPolicy.GetMaxInterval())
			watcher.trimDeletedLabelsByStatus(ctxWithTimeout)

			cancelFunc() // cancel child ctx and is used to cleanup resources once context expires or update is done.
		}
	}
}

func (watcher *managedClusterLabelsStatusWatcher) trimDeletedLabelsByStatus(ctx context.Context) bool {
	leafHubToLabelsSpecBundleMap, err := watcher.specDB.GetEntriesWithDeletedLabels(ctx, watcher.labelsSpecTableName)
	if err != nil {
		watcher.log.Error(err, "trimming cycle skipped")
		return false
	}
	// remove entries with no LH name (temporary state)
	delete(leafHubToLabelsSpecBundleMap, "") // TODO: once non-k8s-restapi exposes hub names, remove line.
	// since we have multiple objects and a success/fail must be returned for interval policy, we should evaluate
	// if the majority passed, and reset if the majority failed.
	successRate := 0
	// iterate over entries
	for _, managedClusterLabelsSpecBundle := range leafHubToLabelsSpecBundleMap {
		// fetch actual labels status reflected in status DB
		for _, managedClusterLabelsSpec := range managedClusterLabelsSpecBundle.Objects {
			labelsStatus, err := watcher.statusDB.GetManagedClusterLabelsStatus(ctx, watcher.labelsStatusTableName,
				managedClusterLabelsSpecBundle.LeafHubName, managedClusterLabelsSpec.ClusterName)
			if err != nil {
				watcher.log.Error(err, "skipped trimming managed cluster labels spec",
					"leaf hub", managedClusterLabelsSpecBundle.LeafHubName,
					"managed cluster", managedClusterLabelsSpec.ClusterName,
					"version", managedClusterLabelsSpec.Version)
				successRate--

				continue
			}

			// check which deleted label keys still appear in status
			deletedLabelsStillInStatus := make([]string, 0)

			for _, key := range managedClusterLabelsSpec.DeletedLabelKeys {
				if _, found := labelsStatus[key]; found {
					deletedLabelsStillInStatus = append(deletedLabelsStillInStatus, key)
				}
			}

			// if deleted labels did not change then skip
			if len(deletedLabelsStillInStatus) == len(managedClusterLabelsSpec.DeletedLabelKeys) {
				continue
			}

			if err := watcher.specDB.UpdateDeletedLabelKeys(ctx, watcher.labelsSpecTableName,
				managedClusterLabelsSpec.Version, managedClusterLabelsSpecBundle.LeafHubName,
				managedClusterLabelsSpec.ClusterName, deletedLabelsStillInStatus); err != nil {
				watcher.log.Error(err, "failed to trim deleted_label_keys",
					"leaf hub", managedClusterLabelsSpecBundle.LeafHubName,
					"managed cluster", managedClusterLabelsSpec.ClusterName, "version", managedClusterLabelsSpec.Version)
				successRate--

				continue
			}

			watcher.log.Info("trimmed labels successfully", "leaf hub", managedClusterLabelsSpecBundle.LeafHubName,
				"managed cluster", managedClusterLabelsSpec.ClusterName, "version", managedClusterLabelsSpec.Version)
			successRate++
		}
	}

	return successRate > 0
}

// TODO: once non-k8s-restapi exposes hub names, remove line.
func (watcher *managedClusterLabelsStatusWatcher) fillMissingLeafHubNames(ctx context.Context) bool {
	entries, err := watcher.specDB.GetEntriesWithoutLeafHubName(ctx, watcher.labelsSpecTableName)
	if err != nil {
		watcher.log.Error(err, "failed to fetch entries with no leaf-hub-name from spec db table", "table",
			watcher.labelsSpecTableName)

		return false
	}

	// since we have multiple objects and a success/fail must be returned for interval policy, we should evaluate
	// if the majority passed, and reset if the majority failed.
	successRate := 0

	// update leaf hub name for each entry
	for _, managedClusterLabelsSpec := range entries {
		leafHubName, err := watcher.statusDB.GetManagedClusterLeafHubName(ctx,
			watcher.labelsStatusTableName, managedClusterLabelsSpec.ClusterName)
		if err != nil {
			watcher.log.Error(err, "failed to get leaf-hub name from status db table",
				"table", watcher.labelsStatusTableName, "managed cluster name", managedClusterLabelsSpec.ClusterName,
				"version", managedClusterLabelsSpec.Version)
			successRate--

			continue
		}

		// update leaf hub name
		if err := watcher.specDB.UpdateLeafHubNames(ctx, watcher.labelsSpecTableName,
			managedClusterLabelsSpec.Version, managedClusterLabelsSpec.ClusterName, leafHubName); err != nil {
			watcher.log.Error(err, "failed to update leaf hub name for managed cluster in spec db table",
				"table", watcher.labelsSpecTableName, "managed cluster name", managedClusterLabelsSpec.ClusterName,
				"version", managedClusterLabelsSpec.Version, "leaf hub name", leafHubName)
			successRate--

			continue
		}

		successRate++

		watcher.log.Info("updated leaf hub name for managed cluster in spec db table",
			"table", watcher.labelsSpecTableName, "managed cluster name", managedClusterLabelsSpec.ClusterName,
			"leaf hub name", leafHubName, "version", managedClusterLabelsSpec.Version)
	}

	return successRate > 0
}
