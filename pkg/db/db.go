package db

import (
	dataTypes "github.com/open-cluster-management/hub-of-hubs-transport-bridge/pkg/data-types"
	"time"
)

type HubOfHubsDb interface {
	GetPoliciesBundle() (*dataTypes.PoliciesBundle, *time.Time, error)
	GetPoliciesLastUpdateTimestamp() (*time.Time, error)
}