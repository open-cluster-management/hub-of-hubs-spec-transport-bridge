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
	"github.com/stolostron/hub-of-hubs-kafka-transport/headers"
	kafkaproducer "github.com/stolostron/hub-of-hubs-kafka-transport/kafka-client/kafka-producer"
	"github.com/stolostron/hub-of-hubs-message-compression/compressors"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/transport"
)

const (
	envVarKafkaProducerID       = "KAFKA_PRODUCER_ID"
	envVarKafkaBootstrapServers = "KAFKA_BOOTSTRAP_SERVERS"
	envVarKafkaTopic            = "KAFKA_TOPIC"
	envVarMessageSizeLimit      = "KAFKA_MESSAGE_SIZE_LIMIT_KB"

	maxMessageSizeLimit = 987 // to make sure that the message size is below 1 MB.
	partition           = 0
	kiloBytesToBytes    = 1000
)

var (
	errEnvVarNotFound     = errors.New("environment variable not found")
	errEnvVarIllegalValue = errors.New("environment variable illegal value")
)

// NewProducer returns a new instance of Producer object.
func NewProducer(compressor compressors.Compressor, log logr.Logger) (*Producer, error) {
	deliveryChan := make(chan kafka.Event)

	kafkaConfigMap, topic, messageSizeLimit, err := readEnvVars()
	if err != nil {
		close(deliveryChan)
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	kafkaProducer, err := kafkaproducer.NewKafkaProducer(kafkaConfigMap, messageSizeLimit*kiloBytesToBytes,
		deliveryChan)
	if err != nil {
		close(deliveryChan)
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
	if err != nil || messageSizeLimit <= 0 {
		return nil, "", 0, fmt.Errorf("%w: %s", errEnvVarIllegalValue, envVarMessageSizeLimit)
	}

	if messageSizeLimit > maxMessageSizeLimit {
		return nil, "", 0, fmt.Errorf("%w - size must not exceed %d : %s", errEnvVarIllegalValue,
			maxMessageSizeLimit, envVarMessageSizeLimit)
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

// Start starts kafka producer.
func (p *Producer) Start() {
	p.startOnce.Do(func() {
		go p.deliveryReportHandler()
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
	msg := &transport.Message{
		ID:      id,
		MsgType: msgType,
		Version: version,
		Payload: payload,
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		p.log.Error(err, "Failed to send message", "MessageId", msg.ID, "MessageType", msg.MsgType,
			"Version", msg.Version)

		return
	}

	compressedBytes, err := p.compressor.Compress(msgBytes)
	if err != nil {
		p.log.Error(err, "Failed to compress bundle", "CompressorType", p.compressor.GetType(),
			"MessageId", msg.ID, "MessageType", msg.MsgType, "Version", msg.Version)

		return
	}

	messageHeaders := []kafka.Header{
		{Key: headers.CompressionType, Value: []byte(p.compressor.GetType())},
	}

	if err = p.kafkaProducer.ProduceAsync(msg.ID, p.topic, partition, messageHeaders, compressedBytes); err != nil {
		p.log.Error(err, "Failed to send message", "MessageId", msg.ID, "MessageType", msg.MsgType,
			"Version", msg.Version)
	}

	p.log.Info("Message sent successfully", "MessageId", msg.ID, "MessageType", msg.MsgType,
		"Version", msg.Version)
}

// GetVersion returns an empty string if the object doesn't exist or an error occurred.
func (p *Producer) GetVersion(_ string, _ string) string {
	return ""
}

func (p *Producer) deliveryReportHandler() {
	for {
		select {
		case <-p.stopChan:
			return

		case event := <-p.deliveryChan:
			p.handleDeliveryReport(event)
		}
	}
}

// handleDeliveryReport handles results of sent messages. For now failed messages are only logged.
func (p *Producer) handleDeliveryReport(kafkaEvent kafka.Event) {
	switch event := kafkaEvent.(type) {
	case *kafka.Message:
		if event.TopicPartition.Error != nil {
			p.log.Error(event.TopicPartition.Error, "Failed to deliver message", "MessageId",
				string(event.Key), "TopicPartition", event.TopicPartition)
		}
	default:
		p.log.Info("Received unsupported kafka-event type", "EventType", event)
	}
}
