package db

import (
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/hub-of-hubs-transport-bridge/pkg/bundle"
	"time"
)

type HubOfHubsDb interface {
	GetBundle(tableName string, createObjFunc bundle.CreateObjectFunction, intoBundle datatypes.Bundle) (*time.Time, error)
	GetLastUpdateTimestamp(tableName string) (*time.Time, error)
}