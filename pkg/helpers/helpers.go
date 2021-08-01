package helpers

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// SetMetaDataAnnotation sets metadata annotation on the given object.
func SetMetaDataAnnotation(object metav1.Object, key string, value string) {
	annotations := object.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	annotations[key] = value

	object.SetAnnotations(annotations)
}

// ContainsString returns true if the string exists in the array and false otherwise
func ContainsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}

	return false
}