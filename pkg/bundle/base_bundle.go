package bundle

import (
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/helpers"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NewBaseBundle creates a new base bundle with no data in it.
func NewBaseBundle() Bundle {
	return &baseBundle{
		Objects:        make([]metav1.Object, 0),
		DeletedObjects: make([]metav1.Object, 0),
	}
}

// manipulate custom is used to manipulate specific fields that are relevant to the specific object
// manipulate function will call it before manipulating name and namespace.
type baseBundle struct {
	Objects        []metav1.Object `json:"objects"`
	DeletedObjects []metav1.Object `json:"deletedObjects"`
}

// AddObject adds an object to the bundle.
func (b *baseBundle) AddObject(object metav1.Object, objectUID string) {
	helpers.SetMetaDataAnnotation(object, datatypes.OriginOwnerReferenceAnnotation, objectUID)
	b.Objects = append(b.Objects, object)
}

// AddDeletedObject adds a deleted object to the bundle.
func (b *baseBundle) AddDeletedObject(object metav1.Object) {
	b.DeletedObjects = append(b.DeletedObjects, object)
}
