package bundle

import (
	"fmt"
	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
	dataTypes "github.com/open-cluster-management/hub-of-hubs-data-types"
)

type PoliciesBundle struct {
	Policies 			[]*policiesv1.Policy `json:"policies"`
	DeletedPolicies 	[]*policiesv1.Policy `json:"deletedPolicies"`
}

func (bundle *PoliciesBundle) AddPolicy(policy *policiesv1.Policy) {
	bundle.Policies = append(bundle.Policies, policy)
}

func (bundle *PoliciesBundle) AddDeletedPolicy(policy *policiesv1.Policy) {
	bundle.DeletedPolicies = append(bundle.DeletedPolicies, policy)
}

func (bundle *PoliciesBundle) ToGenericBundle() *dataTypes.ObjectsBundle {
	genericBundle := dataTypes.NewObjectBundle()
	for _, policy := range bundle.Policies {
		// manipulate name and namespace to avoid collisions of resources with same name on different ns
		policy.SetName(fmt.Sprintf("%s-hoh-%s", policy.GetName(), policy.GetNamespace()))
		policy.SetNamespace(hohSystemNamespace)
		genericBundle.AddObject(policy)
	}
	for _, policy := range bundle.DeletedPolicies {
		// manipulate name and namespace to avoid collisions of resources with same name on different ns
		policy.SetName(fmt.Sprintf("%s-hoh-%s", policy.GetName(), policy.GetNamespace()))
		policy.SetNamespace(hohSystemNamespace)
		genericBundle.AddDeletedObject(policy)
	}
	return genericBundle
}