package db

import (
	dataTypes "github.com/open-cluster-management/hub-of-hubs-transport-bridge/pkg/bundle"
	"time"
)

// TableType describes the CR types supported by the db
type AcmType string

const (
	Policy AcmType = "Policy"
	PlacementRule AcmType = "PlacementRule"
	PlacementBinding AcmType = "PlacementBinding"
)


type HubOfHubsDb interface {
	GetPoliciesBundle() (*dataTypes.PoliciesBundle, *time.Time, error)
	GetPlacementRulesBundle() (*dataTypes.PlacementRulesBundle, *time.Time, error)
	GetPlacementBindingsBundle() (*dataTypes.PlacementBindingsBundle, *time.Time, error)
	GetLastUpdateTimestamp(acmType AcmType) (*time.Time, error)
}