package kafka

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	envVarKafkaProducerID       = "KAFKA_PRODUCER_ID"
	envVarKafkaBootstrapServers = "KAFKA_BOOTSTRAP_SERVERS"
	envVarKafkaTopic            = "KAFKA_TOPIC"
	envVarMessageSizeLimit      = "KAFKA_MESSAGE_SIZE_LIMIT_KB"
	maxMessageSizeLimit         = 987 // to make sure that the message size is below 1 MB.
)

var (
	errEnvVarNotFound     = errors.New("environment variable not found")
	errEnvVarIllegalValue = errors.New("environment variable illegal value")
)

func getKafkaConfig() (*kafka.ConfigMap, string, int, error) {
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
