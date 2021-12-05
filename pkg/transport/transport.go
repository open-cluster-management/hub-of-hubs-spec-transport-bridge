package transport

// Transport is the transport layer interface to be consumed by the spec transport bridge.
type Transport interface {
	// SendAsync sends a message to the transport component asynchronously.
	SendAsync(id string, msgType string, version string, payload []byte)
	// GetVersion returns the latest version of a message from transport.
	GetVersion(id string, msgType string) string
	// Start starts the transport.
	Start()
	// Stop stops the transport.
	Stop()
}
