package bundle

import (
	appsv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/apps/v1"
	dataTypes "github.com/open-cluster-management/hub-of-hubs-data-types"
)

type PlacementRulesBundle struct {
	PlacementRules 			[]*appsv1.PlacementRule `json:"placementRules"`
	DeletedPlacementRules 	[]*appsv1.PlacementRule  `json:"deletedPlacementRules"`
}

func (bundle *PlacementRulesBundle) AddPlacementRule(placementRule *appsv1.PlacementRule) {
	bundle.PlacementRules = append(bundle.PlacementRules, placementRule)
}

func (bundle *PlacementRulesBundle) AddDeletedPlacementRule(placementRule *appsv1.PlacementRule) {
	bundle.DeletedPlacementRules = append(bundle.DeletedPlacementRules, placementRule)
}

func (bundle *PlacementRulesBundle) ToGenericBundle() *dataTypes.ObjectsBundle {
	genericBundle := dataTypes.NewObjectBundle()
	for _, placementRule := range bundle.PlacementRules {
		genericBundle.AddObject(placementRule)
	}
	for _, placementRule := range bundle.DeletedPlacementRules {
		genericBundle.AddDeletedObject(placementRule)
	}
	return genericBundle
}
