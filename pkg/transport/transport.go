package transport

type Transport interface {
	Send(id string, msgType string, version string, payload []byte)
	GetVersion(id string, msgType string) string
}
