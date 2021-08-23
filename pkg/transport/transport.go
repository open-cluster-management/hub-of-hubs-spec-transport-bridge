package transport

// Transport is the transport layer interface to be consumed by the spec transport bridge.
type Transport interface {
	SendAsync(id string, msgType string, version string, payload []byte)
	GetVersion(id string, msgType string) string
	Start(stopChannel <-chan struct{}) error
}
