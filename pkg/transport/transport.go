package transport

// Transport is the transport layer interface to be consumed by the spec transport bridge.
type Transport interface {
	// SendAsync sends a message to the transport layer asynchronously.
	SendAsync(id string, msgType string, version string, payload []byte)
	// GetVersion returns the version of an object, or an empty string if the object doesn't exist or an error occurred.
	GetVersion(id string, msgType string) string
	// Start starts the transport (implementing Runnable interface).
	Start(stopChannel <-chan struct{}) error
}
