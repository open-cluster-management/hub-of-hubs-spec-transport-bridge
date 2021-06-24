package bundle

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type CreateObjectFunction func() metav1.Object
type CreateBundleFunction func() Bundle
type ManipulateCustomFunction func(object metav1.Object)

type Bundle interface {
	AddObject(object metav1.Object, objectUID string)
	AddDeletedObject(object metav1.Object)
}
