package kafkaclient

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	kclient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-producer"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
)

// NewProducer returns a new instance of Producer object.
func NewProducer(log logr.Logger) (*Producer, error) {
	kp := &Producer{
		deliveryChan:  make(chan kafka.Event),
		stopChan:      make(chan struct{}, 1),
		kafkaProducer: nil,
		log:           log,
	}

	kafkaProducer, err := kclient.NewKafkaProducer(kp.deliveryChan)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	kp.kafkaProducer = kafkaProducer

	return kp, nil
}

// Producer abstracts hub-of-hubs-kafka-transport kafka-producer's generic usage.
type Producer struct {
	log           logr.Logger
	kafkaProducer *kclient.KafkaProducer
	deliveryChan  chan kafka.Event
	stopChan      chan struct{}
	startOnce     sync.Once
	stopOnce      sync.Once
}

// deliveryHandler handles results of sent messages.
// For now failed messages are only logged.
func (p *Producer) deliveryHandler(kafkaEvent *kafka.Event) {
	switch event := (*kafkaEvent).(type) {
	case *kafka.Message:
		if event.TopicPartition.Error != nil {
			load := &transport.Message{}

			err := json.Unmarshal(event.Value, load)
			if err != nil {
				p.log.Error(err, "Failed to deliver message",
					"Topic Name", event.TopicPartition)
				return
			}

			p.log.Error(event.TopicPartition.Error, "Failed to deliver message",
				"Message ID", load.ID, "Topic Name", event.TopicPartition)
		}
	default:
		p.log.Info("Received unsupported kafka-event type", "Message Type", event)
	}
}

// Start starts the kafka-client.
func (p *Producer) Start() {
	p.startOnce.Do(func() {
		// Delivery report handler for produced messages
		go func() {
			for {
				select {
				case <-p.stopChan:
					return
				case e := <-p.deliveryChan:
					p.deliveryHandler(&e)
				}
			}
		}()
	})
}

// Stop stops the kafka-client.
func (p *Producer) Stop() {
	p.stopOnce.Do(func() {
		p.kafkaProducer.Close()
		p.stopChan <- struct{}{}
		close(p.stopChan)
		close(p.deliveryChan)
	})
}

// SendAsync sends a message to the sync service asynchronously.
func (p *Producer) SendAsync(id string, msgType string, version string, payload []byte) {
	message := &transport.Message{
		ID:      id,
		MsgType: msgType,
		Version: version,
		Payload: payload,
	}

	messageBytes, err := json.Marshal(message)
	if err != nil {
		p.log.Error(err, "Failed to send message", "Message ID", message.ID)
		return
	}

	if err = p.kafkaProducer.ProduceAsync(&messageBytes); err != nil {
		p.log.Error(err, "Failed to send message", "Message ID", message.ID)
	}
}

// GetVersion returns an empty string if the object doesn't exist or an error occurred.
func (p *Producer) GetVersion(id string, msgType string) string {
	// TODO: implement with consumer
	return ""
}
