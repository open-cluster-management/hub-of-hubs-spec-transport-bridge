package bundle

import (
	"errors"

	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var errBadType = errors.New("ObjectBundle type mismatch, should be a baseBundle")

// NewBaseBundle creates a new base bundle with no data in it.
func NewBaseBundle() ObjectsBundle {
	return &baseBundle{
		Objects:        make([]metav1.Object, 0),
		DeletedObjects: make([]metav1.Object, 0),
	}
}

type baseBundle struct {
	Objects        []metav1.Object `json:"objects"`
	DeletedObjects []metav1.Object `json:"deletedObjects"`
}

// AddObject adds an object to the bundle.
func (b *baseBundle) AddObject(object metav1.Object, objectUID string) {
	setMetaDataAnnotation(object, datatypes.OriginOwnerReferenceAnnotation, objectUID)
	b.Objects = append(b.Objects, object)
}

// AddDeletedObject adds a deleted object to the bundle.
func (b *baseBundle) AddDeletedObject(object metav1.Object) {
	b.DeletedObjects = append(b.DeletedObjects, object)
}

// MergeBundle merges the content of another ObjectsBundle into the callee.
func (b *baseBundle) MergeBundle(other ObjectsBundle) error {
	if b == other {
		return nil // don't do anything
	}

	otherBaseBundle, ok := other.(*baseBundle)
	if !ok {
		return errBadType // shouldn't happen
	}

	b.Objects = append(b.Objects, otherBaseBundle.Objects...)
	b.DeletedObjects = append(b.DeletedObjects, otherBaseBundle.DeletedObjects...)

	return nil
}

// setMetaDataAnnotation sets metadata annotation on the given object.
func setMetaDataAnnotation(object metav1.Object, key string, value string) {
	annotations := object.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	annotations[key] = value

	object.SetAnnotations(annotations)
}
