package bundle

import (
	"fmt"

	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NewPlacementBindingBundle creates a new placement binding bundle with no data in it.
func NewPlacementBindingBundle() Bundle {
	return &baseBundle{
		Objects:              make([]metav1.Object, 0),
		DeletedObjects:       make([]metav1.Object, 0),
		manipulateCustomFunc: manipulateCustom,
	}
}

func manipulateCustom(object metav1.Object) {
	placementBinding, ok := object.(*policiesv1.PlacementBinding)
	if !ok {
		return
	}

	namespace := placementBinding.GetNamespace()
	placementBinding.PlacementRef.Name = fmt.Sprintf("%s-hoh-%s", placementBinding.PlacementRef.Name, namespace)

	for i, subject := range placementBinding.Subjects {
		placementBinding.Subjects[i].Name = fmt.Sprintf("%s-hoh-%s", subject.Name, namespace)
	}
}
