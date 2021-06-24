package bundle

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	hohSystemNamespace      = "hoh-system"
	objectUidAnnotationName = "hub-of-hubs.open-cluster-management.io/originObjectUid"
)

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

func (b *baseBundle) AddObject(object metav1.Object, objectUID string) {
	b.Objects = append(b.Objects, b.manipulate(object, objectUID))
}

func (b *baseBundle) AddDeletedObject(object metav1.Object, objectUID string) {
	b.DeletedObjects = append(b.DeletedObjects, b.manipulate(object, objectUID))
}

func (b *baseBundle) manipulate(object metav1.Object, objectUID string) metav1.Object {
	b.manipulateCustomFunc(object)
	b.manipulateCommon(object, objectUID)
	return object
}

// manipulate name and namespace to avoid collisions of resources with same name on different ns.
// add annotation with the HoH object id - to be able to reference the exact origin object
func (b *baseBundle) manipulateCommon(object metav1.Object, objectUID string) {
	object.SetName(fmt.Sprintf("%s-hoh-%s", object.GetName(), object.GetNamespace()))
	object.SetNamespace(hohSystemNamespace)
	b.setMetaDataAnnotation(object, objectUidAnnotationName, objectUID)
}

func (b *baseBundle) setMetaDataAnnotation(object metav1.Object, key string, value string) {
	annotations := object.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[key] = value
	object.SetAnnotations(annotations)
}
