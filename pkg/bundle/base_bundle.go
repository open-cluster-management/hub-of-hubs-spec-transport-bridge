package bundle

import (
	"fmt"

	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/helpers"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NewBaseBundle creates a new base bundle with no data in it.
func NewBaseBundle() Bundle {
	return &baseBundle{
		Objects:              make([]metav1.Object, 0),
		DeletedObjects:       make([]metav1.Object, 0),
		manipulateCustomFunc: func(object metav1.Object) {},
	}
}

// manipulate custom is used to manipulate specific fields that are relevant to the specific object
// manipulate function will call it before manipulating name and namespace.
type baseBundle struct {
	Objects              []metav1.Object `json:"objects"`
	DeletedObjects       []metav1.Object `json:"deletedObjects"`
	manipulateCustomFunc ManipulateCustomFunction
}

// AddObject adds an object to the bundle.
func (b *baseBundle) AddObject(object metav1.Object, objectUID string) {
	helpers.SetMetaDataAnnotation(object, datatypes.OriginOwnerReferenceAnnotation, objectUID)
	b.Objects = append(b.Objects, b.manipulate(object))
}

// AddDeletedObject adds a deleted object to the bundle.
func (b *baseBundle) AddDeletedObject(object metav1.Object) {
	b.DeletedObjects = append(b.DeletedObjects, b.manipulate(object))
}

func (b *baseBundle) manipulate(object metav1.Object) metav1.Object {
	if object.GetNamespace() == datatypes.HohSystemNamespace {
		return object
	}

	b.manipulateCustomFunc(object)
	b.manipulateNameAndNamespace(object)

	return object
}

// manipulate name and namespace to avoid collisions of resources with same name on different ns.
// manipulate objects only if they were created on user namespaces. don't manipulate on hoh-system ns.
func (b *baseBundle) manipulateNameAndNamespace(object metav1.Object) {
	object.SetName(fmt.Sprintf("%s-hoh-%s", object.GetName(), object.GetNamespace()))
	object.SetNamespace(datatypes.HohSystemNamespace)
}
