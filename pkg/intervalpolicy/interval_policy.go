package intervalpolicy

import "time"

// IntervalPolicy defines a policy to return interval based on the received events.
type IntervalPolicy interface {
	Evaluate()
	Reset()
	GetInterval() time.Duration
}
