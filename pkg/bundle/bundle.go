package bundle

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type (
	// CreateObjectFunction is a function that specifies how to create an object.
	CreateObjectFunction func() metav1.Object
	// CreateBundleFunction is a function that specifies how to create a bundle.
	CreateBundleFunction func() Bundle
	// ManipulateCustomFunction is a function that specifies how to do customized manipulation to a specific object.
	ManipulateCustomFunction func(object metav1.Object)
)

// Bundle bundles together a set of k8s objects to be sent to leaf hubs via transport layer.
type Bundle interface {
	// AddObject adds an object to the bundle.
	AddObject(object metav1.Object, objectUID string)
	// AddDeletedObject adds a deleted object to the bundle.
	AddDeletedObject(object metav1.Object)
}
