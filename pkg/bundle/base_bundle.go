package bundle

import (
	"fmt"
	dataTypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const hohSystemNamespace = "hoh-system"

func NewBaseBundle() Bundle {
	return &BaseBundle{
		Objects:              make([]metav1.Object, 0),
		DeletedObjects:       make([]metav1.Object, 0),
		manipulateCustomFunc: func(object metav1.Object) { },
	}
}

// manipulate custom is used to manipulate specific fields that are relevant to the specific object
// manipulate function will call it before manipulating name and namespace.
type BaseBundle struct {
	Objects              []metav1.Object `json:"objects"`
	DeletedObjects       []metav1.Object `json:"deletedObjects"`
	manipulateCustomFunc ManipulateCustomFunction
}

func (b *BaseBundle) AddObject(object metav1.Object) {
	b.Objects = append(b.Objects, b.manipulate(object))
}

func (b *BaseBundle) AddDeletedObject(object metav1.Object) {
	b.DeletedObjects = append(b.DeletedObjects, b.manipulate(object))
}

func (b *BaseBundle) manipulate(object metav1.Object) metav1.Object {
	b.manipulateCustomFunc(object)
	b.manipulateNameAndNamespace(object)
	return object
}

// manipulate name and namespace to avoid collisions of resources with same name on different ns
func (b *BaseBundle) manipulateNameAndNamespace(object metav1.Object) {
	object.SetName(fmt.Sprintf("%s-hoh-%s", object.GetName(), object.GetNamespace()))
	object.SetNamespace(hohSystemNamespace)
}

func (b *BaseBundle) ToGenericBundle() *dataTypes.ObjectsBundle {
	genericBundle := dataTypes.NewObjectBundle()
	for _, object := range b.Objects {
		genericBundle.AddObject(object)
	}
	for _, object := range b.DeletedObjects {
		genericBundle.AddDeletedObject(object)
	}
	return genericBundle
}
