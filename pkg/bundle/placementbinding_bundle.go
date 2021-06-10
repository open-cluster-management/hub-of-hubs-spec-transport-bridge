package bundle

import (
	"fmt"
	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
	"github.com/open-cluster-management/hub-of-hubs-data-types"
)

func NewPlacementBindingBundle() datatypes.Bundle {
	return &baseBundle{
		ObjectsBundle: datatypes.NewObjectBundle(),
		manipulateCustomFunc: manipulateCustom,
	}
}

func manipulateCustom(object datatypes.Object) {
	placementBinding := object.(*policiesv1.PlacementBinding)
	namespace := placementBinding.GetNamespace()
	placementBinding.PlacementRef.Name = fmt.Sprintf("%s-hoh-%s", placementBinding.PlacementRef.Name, namespace)
	for i, subject := range placementBinding.Subjects {
		placementBinding.Subjects[i].Name = fmt.Sprintf("%s-hoh-%s", subject.Name, namespace)
	}
}