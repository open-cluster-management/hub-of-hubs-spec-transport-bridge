package dbsyncer

import (
	"time"
)

const (
	maxNumOfConsecutiveSyncs = 3
	maxNumOfIncrements       = 3
	floatingFactor           = 2
)

// syncerIntervalPolicy defines a policy to return syncer "floating" interval.
type syncerIntervalPolicy interface {
	onSyncPerformed()
	onSyncSkipped()
	getInterval() time.Duration
}

// defaultSyncerIntervalPolicy is a default syncer "floating" interval policy.
type defaultSyncerIntervalPolicy struct {
	originalInterval      time.Duration
	floatingInterval      time.Duration
	numOfConsecutiveSyncs int64
	numOfIncrements       int64
}

func newDefaultSyncerIntervalPolicy(interval time.Duration) *defaultSyncerIntervalPolicy {
	return &defaultSyncerIntervalPolicy{originalInterval: interval, floatingInterval: interval}
}

// onSyncPerformed recalculates floating interval if sync happened.
func (policy *defaultSyncerIntervalPolicy) onSyncPerformed() {
	// we've reached maximum number of interval's increments due to consecutive syncs - there is nothing to do
	if policy.numOfIncrements == maxNumOfIncrements {
		return
	}

	policy.numOfConsecutiveSyncs++

	// we've reached maximum number of consecutive sync - increment the interval
	if policy.numOfConsecutiveSyncs == maxNumOfConsecutiveSyncs {
		policy.floatingInterval *= floatingFactor
		policy.numOfConsecutiveSyncs = 0
		policy.numOfIncrements++
	}
}

// onSyncSkipped resets the entire state of the policy if sync was skipped.
func (policy *defaultSyncerIntervalPolicy) onSyncSkipped() {
	policy.numOfConsecutiveSyncs = 0
	policy.numOfIncrements = 0
	policy.floatingInterval = policy.originalInterval
}

func (policy *defaultSyncerIntervalPolicy) getInterval() time.Duration {
	return policy.floatingInterval
}
