package transport

// Message abstracts a message object to be used by different transport components.
type Message struct {
	ID      string `json:"id"`
	MsgType string `json:"msg_type"`
	Version string `json:"version"`
	Payload []byte `json:"payload"`
}
