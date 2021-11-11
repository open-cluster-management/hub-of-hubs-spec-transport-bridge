package intervalpolicy

import (
	"time"

	"github.com/cenkalti/backoff/v4"
)

// exponentialBackoffIntervalPolicy is a default interval policy.
type exponentialBackoffIntervalPolicy struct {
	exponentialBackoff *backoff.ExponentialBackOff
	interval           time.Duration
}

// NewExponentialBackoffIntervalPolicy creates new exponential backoff interval policy.
func NewExponentialBackoffIntervalPolicy(interval time.Duration) IntervalPolicy {
	intervalPolicy := &exponentialBackoffIntervalPolicy{exponentialBackoff: backoff.NewExponentialBackOff()}

	intervalPolicy.exponentialBackoff.InitialInterval = interval
	intervalPolicy.interval = interval

	return intervalPolicy
}

// Evaluate reevaluates interval.
func (policy *exponentialBackoffIntervalPolicy) Evaluate() {
	policy.interval = policy.exponentialBackoff.NextBackOff()
}

// Reset resets the entire state of the policy.
func (policy *exponentialBackoffIntervalPolicy) Reset() {
	policy.exponentialBackoff.Reset()
}

// GetInterval returns reevaluated interval.
func (policy *exponentialBackoffIntervalPolicy) GetInterval() time.Duration {
	return policy.interval
}
