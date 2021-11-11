package intervalpolicy

import (
	"time"

	"github.com/cenkalti/backoff/v4"
)

const (
	baseFactor                                      = 2
	maxInterval                                     = 60 * time.Second
	maxNumOfConsecutiveEvaluationsBeforeNextBackoff = 3
)

// exponentialBackoffIntervalPolicy is a default interval policy.
type exponentialBackoffIntervalPolicy struct {
	exponentialBackoff          *backoff.ExponentialBackOff
	interval                    time.Duration
	numOfConsecutiveEvaluations int
}

// NewExponentialBackoffIntervalPolicy creates new exponential backoff interval policy.
func NewExponentialBackoffIntervalPolicy(interval time.Duration) IntervalPolicy {
	exponentialBackoff := &backoff.ExponentialBackOff{
		InitialInterval:     interval,
		RandomizationFactor: 0,
		Multiplier:          baseFactor,
		MaxInterval:         maxInterval,
		MaxElapsedTime:      0,
		Stop:                0,
		Clock:               backoff.SystemClock,
	}

	exponentialBackoff.Reset()

	return &exponentialBackoffIntervalPolicy{
		exponentialBackoff: exponentialBackoff,
		interval:           interval,
	}
}

// Evaluate reevaluates interval.
func (policy *exponentialBackoffIntervalPolicy) Evaluate() {
	policy.numOfConsecutiveEvaluations++

	if policy.numOfConsecutiveEvaluations == maxNumOfConsecutiveEvaluationsBeforeNextBackoff {
		policy.interval = policy.exponentialBackoff.NextBackOff()
		policy.numOfConsecutiveEvaluations = 0
	}
}

// Reset resets the entire state of the policy.
func (policy *exponentialBackoffIntervalPolicy) Reset() {
	policy.exponentialBackoff.Reset()
	policy.interval = policy.exponentialBackoff.InitialInterval
	policy.numOfConsecutiveEvaluations = 0
}

// GetInterval returns reevaluated interval.
func (policy *exponentialBackoffIntervalPolicy) GetInterval() time.Duration {
	return policy.interval
}
