package kafkaclient

import (
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	kclient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-producer"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
)

// NewKafkaProducer returns a new instance of KafkaProducer object.
func NewKafkaProducer(log logr.Logger) (*KafkaProducer, error) {
	kp := &KafkaProducer{
		deliveryChan:  make(chan kafka.Event, 1000),
		stopChan:      make(chan struct{}, 1),
		kafkaProducer: nil,
		log:           log,
	}

	kafkaProducer, err := kclient.NewKafkaProducer(kp.deliveryChan)
	if err != nil {
		return nil, err
	}

	kp.kafkaProducer = kafkaProducer
	return kp, nil
}

// KafkaProducer abstracts hub-of-hubs-kafka-transport kafka-producer's generic usage.
type KafkaProducer struct {
	kafkaProducer *kclient.KafkaProducer
	deliveryChan  chan kafka.Event
	stopChan      chan struct{}
	log           logr.Logger
}

// deliveryHandler handles results of sent messages.
// For now failed messages are only logged
func (p *KafkaProducer) deliveryHandler(e *kafka.Event) {
	switch ev := (*e).(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			load := &transport.Message{}
			err := json.Unmarshal(ev.Value, load)
			if err != nil {
				p.log.Error(ev.TopicPartition.Error, "Failed to deliver message on %v\n",
					ev.TopicPartition)
				return
			}

			p.log.Error(ev.TopicPartition.Error, "Failed to deliver message %v on %v: %v\n",
				load.ID,
				ev.TopicPartition)
		}
	}
}

// Start starts the kafka-client
func (p *KafkaProducer) Start() {
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
}

// Stop stops the kafka-client
func (p *KafkaProducer) Stop() {
	p.kafkaProducer.Close()
	p.stopChan <- struct{}{}
	close(p.stopChan)
	close(p.deliveryChan)
}

// SendAsync sends a message to the sync service asynchronously.
func (p *KafkaProducer) SendAsync(id string, msgType string, version string, payload []byte) {
	message := &transport.Message{
		ID:      id,
		MsgType: msgType,
		Version: version,
		Payload: payload,
	}

	bs, err := json.Marshal(message)
	if err != nil {
		p.log.Error(err, "Failed to send message: %v\n", message.ID)
		return
	}

	err = p.kafkaProducer.ProduceAsync(&bs)
	if err != nil {
		p.log.Error(err, "Failed to send message: %v\n", message.ID)
	}
}

// GetVersion returns an empty string if the object doesn't exist or an error occurred.
func (p *KafkaProducer) GetVersion(id string, msgType string) string {
	// TODO: implement with consumer
	return ""
}
