package db

import (
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"time"
)

type HubOfHubsSpecDB interface {
	GetBundle(tableName string, createObjFunc bundle.CreateObjectFunction, intoBundle bundle.Bundle) (*time.Time, error)
	GetLastUpdateTimestamp(tableName string) (*time.Time, error)
}
