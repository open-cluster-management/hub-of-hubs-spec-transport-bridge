package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	kafkaproducer "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-producer"
	kafkaHeaderTypes "github.com/open-cluster-management/hub-of-hubs-kafka-transport/types"
	"github.com/open-cluster-management/hub-of-hubs-message-compression/compressors"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
)

const (
	envVarKafkaProducerID       = "KAFKA_PRODUCER_ID"
	envVarKafkaBootstrapServers = "KAFKA_BOOTSTRAP_SERVERS"
	envVarKafkaTopic            = "KAFKA_TOPIC"
	envVarMessageSizeLimit      = "KAFKA_MESSAGE_SIZE_LIMIT_KB"

	maxMessageSizeLimit = 987 // to make sure that the message size is below 1 MB.
	bufferedChannelSize = 500
	partition           = 0
	kiloBytesToBytes    = 1000
)

var (
	errEnvVarNotFound     = errors.New("environment variable not found")
	errEnvVarIllegalValue = errors.New("environment variable illegal value")
)

// NewProducer returns a new instance of Producer object.
func NewProducer(compressor compressors.Compressor, log logr.Logger) (*Producer, error) {
	deliveryChan := make(chan kafka.Event, bufferedChannelSize)

	kafkaConfigMap, topic, messageSizeLimit, err := readEnvVars()
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	kafkaProducer, err := kafkaproducer.NewKafkaProducer(kafkaConfigMap, messageSizeLimit*kiloBytesToBytes,
		deliveryChan)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Producer{
		log:           log,
		kafkaProducer: kafkaProducer,
		topic:         topic,
		compressor:    compressor,
		deliveryChan:  deliveryChan,
		stopChan:      make(chan struct{}),
	}, nil
}

func readEnvVars() (*kafka.ConfigMap, string, int, error) {
	producerID, found := os.LookupEnv(envVarKafkaProducerID)
	if !found {
		return nil, "", 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaProducerID)
	}

	bootstrapServers, found := os.LookupEnv(envVarKafkaBootstrapServers)
	if !found {
		return nil, "", 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaBootstrapServers)
	}

	topic, found := os.LookupEnv(envVarKafkaTopic)
	if !found {
		return nil, "", 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaTopic)
	}

	messageSizeLimitString, found := os.LookupEnv(envVarMessageSizeLimit)
	if !found {
		return nil, "", 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarMessageSizeLimit)
	}

	messageSizeLimit, err := strconv.Atoi(messageSizeLimitString)
	if err != nil || messageSizeLimit <= 0 || messageSizeLimit > maxMessageSizeLimit {
		return nil, "", 0, fmt.Errorf("%w: %s", errEnvVarIllegalValue, envVarMessageSizeLimit)
	}

	kafkaConfigMap := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"client.id":         producerID,
		"acks":              "1",
		"retries":           "0",
	}

	return kafkaConfigMap, topic, messageSizeLimit, nil
}

// Producer abstracts hub-of-hubs-kafka-transport kafka-producer's generic usage.
type Producer struct {
	log           logr.Logger
	kafkaProducer *kafkaproducer.KafkaProducer
	topic         string
	compressor    compressors.Compressor
	deliveryChan  chan kafka.Event
	stopChan      chan struct{}
	startOnce     sync.Once
	stopOnce      sync.Once
}

// Start starts the kafka.
func (p *Producer) Start() {
	p.startOnce.Do(func() {
		go p.handleDelivery()
	})
}

// Stop stops the producer.
func (p *Producer) Stop() {
	p.stopOnce.Do(func() {
		p.stopChan <- struct{}{}
		p.kafkaProducer.Close()
		close(p.deliveryChan)
		close(p.stopChan)
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
		p.log.Error(err, "Failed to send message", "MessageId", message.ID, "MessageType",
			message.MsgType, "Version", message.Version)

		return
	}

	compressedBytes, err := p.compressor.Compress(messageBytes)
	if err != nil {
		p.log.Error(err, "Failed to compress bundle", "CompressorType", p.compressor.GetType(),
			"MessageId", message.ID, "MessageType", message.MsgType, "Version", message.Version)

		return
	}

	headers := []kafka.Header{
		{Key: kafkaHeaderTypes.MsgIDKey, Value: []byte(message.ID)},
		{Key: kafkaHeaderTypes.MsgTypeKey, Value: []byte(message.MsgType)},
		{Key: kafkaHeaderTypes.HeaderCompressionType, Value: []byte(p.compressor.GetType())},
	}

	if err = p.kafkaProducer.ProduceAsync(message.ID, p.topic, partition, headers, compressedBytes); err != nil {
		p.log.Error(err, "Failed to send message", "MessageId", message.ID, "MessageType",
			message.MsgType, "Version", message.Version)
	}
}

// GetVersion returns an empty string if the object doesn't exist or an error occurred.
func (p *Producer) GetVersion(_ string, _ string) string {
	return ""
}

func (p *Producer) handleDelivery() {
	for {
		select {
		case <-p.stopChan:
			return

		case event := <-p.deliveryChan:
			p.deliveryHandler(&event)
		}
	}
}

// deliveryHandler handles results of sent messages. For now failed messages are only logged.
func (p *Producer) deliveryHandler(kafkaEvent *kafka.Event) {
	switch event := (*kafkaEvent).(type) {
	case *kafka.Message:
		if event.TopicPartition.Error != nil {
			message := &transport.Message{}

			if err := json.Unmarshal(event.Value, message); err != nil {
				p.log.Error(err, "Failed to deliver message", "MessageKey", string(event.Key),
					"TopicPartition", event.TopicPartition)
				return
			}

			p.log.Error(event.TopicPartition.Error, "Failed to deliver message", "MessageId",
				message.ID, "MessageType", message.MsgType, "Version", message.Version, "TopicPartition",
				event.TopicPartition)
		}
	default:
		p.log.Info("Received unsupported kafka-event type", "EventType", event)
	}
}
