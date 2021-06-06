package bundle

import (
	"fmt"
	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
	dataTypes "github.com/open-cluster-management/hub-of-hubs-data-types"
)

type PlacementBindingsBundle struct {
	PlacementBindings 			[]*policiesv1.PlacementBinding `json:"placementBindings"`
	DeletedPlacementBindings 	[]*policiesv1.PlacementBinding `json:"deletedPlacementBindings"`
}

func (bundle *PlacementBindingsBundle) AddPlacementBinding(placementBinding *policiesv1.PlacementBinding) {
	bundle.PlacementBindings = append(bundle.PlacementBindings, placementBinding)
}

func (bundle *PlacementBindingsBundle) AddDeletedPlacementBinding(placementBinding *policiesv1.PlacementBinding) {
	bundle.DeletedPlacementBindings = append(bundle.DeletedPlacementBindings, placementBinding)
}

func (bundle *PlacementBindingsBundle) ToGenericBundle() *dataTypes.ObjectsBundle {
	genericBundle := dataTypes.NewObjectBundle()
	for _, placementBinding := range bundle.PlacementBindings {
		namespace := placementBinding.GetNamespace()
		placementBinding.SetName(fmt.Sprintf("%s-hoh-%s", placementBinding.GetName(), namespace))
		placementBinding.SetNamespace(hohSystemNamespace)
		placementBinding.PlacementRef.Name = fmt.Sprintf("%s-hoh-%s", placementBinding.PlacementRef.Name, namespace)
		for i, subject := range placementBinding.Subjects {
			placementBinding.Subjects[i].Name = fmt.Sprintf("%s-hoh-%s", subject.Name, namespace)
		}
		genericBundle.AddObject(placementBinding)
	}
	for _, placementBinding := range bundle.DeletedPlacementBindings {
		placementBinding.SetName(fmt.Sprintf("%s-hoh-%s", placementBinding.GetName(), placementBinding.GetNamespace()))
		placementBinding.SetNamespace(hohSystemNamespace)
		genericBundle.AddDeletedObject(placementBinding)
	}
	return genericBundle
}
