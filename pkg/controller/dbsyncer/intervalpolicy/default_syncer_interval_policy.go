package intervalpolicy

import (
	"time"
)

const (
	maxNumOfConsecutiveSyncs = 3
	maxNumOfIncrements       = 3
	floatingFactor           = 2
)

// defaultSyncerIntervalPolicy is a default syncer "floating" interval policy.
type defaultSyncerIntervalPolicy struct {
	originalInterval      time.Duration
	floatingInterval      time.Duration
	numOfConsecutiveSyncs int64
	numOfIncrements       int64
}

// NewDefaultSyncerIntervalPolicy creates new default syncer interval policy.
func NewDefaultSyncerIntervalPolicy(interval time.Duration) SyncerIntervalPolicy {
	return &defaultSyncerIntervalPolicy{originalInterval: interval, floatingInterval: interval}
}

// OnSyncPerformed recalculates floating interval if sync happened.
func (policy *defaultSyncerIntervalPolicy) OnSyncPerformed() {
	// we've reached maximum number of interval's increments due to consecutive syncs - there is nothing to do
	if policy.numOfIncrements == maxNumOfIncrements {
		return
	}

	policy.numOfConsecutiveSyncs++

	// we've reached maximum number of consecutive syncs - increment the interval
	if policy.numOfConsecutiveSyncs == maxNumOfConsecutiveSyncs {
		policy.floatingInterval *= floatingFactor
		policy.numOfConsecutiveSyncs = 0
		policy.numOfIncrements++
	}
}

// OnSyncSkipped resets the entire state of the policy if sync was skipped.
func (policy *defaultSyncerIntervalPolicy) OnSyncSkipped() {
	policy.numOfConsecutiveSyncs = 0
	policy.numOfIncrements = 0
	policy.floatingInterval = policy.originalInterval
}

// GetInterval returns recalculated interval bases on the received events.
func (policy *defaultSyncerIntervalPolicy) GetInterval() time.Duration {
	return policy.floatingInterval
}
