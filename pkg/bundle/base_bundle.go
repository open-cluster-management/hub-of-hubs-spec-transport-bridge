package bundle

import (
	"fmt"
	"github.com/open-cluster-management/hub-of-hubs-data-types"
)

const hohSystemNamespace = "hoh-system"

type CreateObjectFunction func() datatypes.Object
type CreateBundleFunction func() datatypes.Bundle
type ManipulateCustomFunction func(object datatypes.Object)

func NewBaseBundle() datatypes.Bundle {
	return &baseBundle{
		ObjectsBundle: datatypes.NewObjectBundle(),
		manipulateCustomFunc: func(object datatypes.Object) { },
	}
}

// manipulate custom is used to manipulate specific fields that are relevant to the specific object
// manipulate function will call it before manipulating name and namespace.
type baseBundle struct {
	*datatypes.ObjectsBundle
	manipulateCustomFunc ManipulateCustomFunction
}

func (b *baseBundle) AddObject(object datatypes.Object) {
	b.Objects = append(b.Objects, b.manipulate(object))
}

func (b *baseBundle) AddDeletedObject(object datatypes.Object) {
	b.DeletedObjects = append(b.DeletedObjects, b.manipulate(object))
}

func (b *baseBundle) manipulate(object datatypes.Object) datatypes.Object {
	b.manipulateCustomFunc(object)
	b.manipulateNameAndNamespace(object)
	return object
}

// manipulate name and namespace to avoid collisions of resources with same name on different ns
func (b *baseBundle) manipulateNameAndNamespace(object datatypes.Object) {
	object.SetName(fmt.Sprintf("%s-hoh-%s", object.GetName(), object.GetNamespace()))
	object.SetNamespace(hohSystemNamespace)
}
