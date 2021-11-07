package intervalpolicy

import "time"

// SyncerIntervalPolicy defines a policy to return syncer interval based on the received events.
type SyncerIntervalPolicy interface {
	OnSyncPerformed()
	OnSyncSkipped()
	GetInterval() time.Duration
}
