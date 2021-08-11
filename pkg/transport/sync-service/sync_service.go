package syncservice

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	"github.com/open-horizon/edge-sync-service-client/client"
)

const (
	envVarSyncServiceProtocol = "SYNC_SERVICE_PROTOCOL"
	envVarSyncServiceHost     = "SYNC_SERVICE_HOST"
	envVarSyncServicePort     = "SYNC_SERVICE_PORT"
)

var errEnvVarNotFound = errors.New("not found environment variable")

// NewSyncService returns a new instance of SyncService object.
func NewSyncService(log logr.Logger) (*SyncService, error) {
	serverProtocol, host, port, err := readEnvVars()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sync service - %w", err)
	}

	syncServiceClient := client.NewSyncServiceClient(serverProtocol, host, port)

	syncServiceClient.SetOrgID("myorg")
	syncServiceClient.SetAppKeyAndSecret("user@myorg", "")

	return &SyncService{
		client:   syncServiceClient,
		msgChan:  make(chan *transport.Message),
		stopChan: make(chan struct{}, 1),
		log:      log,
	}, nil
}

// SyncService abstracts Open Horizon Sync Service usage.
type SyncService struct {
	client    *client.SyncServiceClient
	msgChan   chan *transport.Message
	stopChan  chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	log       logr.Logger
}

func readEnvVars() (string, string, uint16, error) {
	protocol, found := os.LookupEnv(envVarSyncServiceProtocol)
	if !found {
		return "", "", 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarSyncServiceProtocol)
	}

	host, found := os.LookupEnv(envVarSyncServiceHost)
	if !found {
		return "", "", 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarSyncServiceHost)
	}

	portString, found := os.LookupEnv(envVarSyncServicePort)
	if !found {
		return "", "", 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarSyncServicePort)
	}

	port, err := strconv.Atoi(portString)
	if err != nil {
		return "", "", 0, fmt.Errorf("the environment var %s is not valid port - %w", envVarSyncServicePort, err)
	}

	return protocol, host, uint16(port), nil
}

// Start starts the sync service.
func (s *SyncService) Start() {
	s.startOnce.Do(func() {
		go s.distributeMessages()
	})
}

// Stop stops the sync service.
func (s *SyncService) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopChan)
	})
}

// SendAsync sends a message to the sync service asynchronously.
func (s *SyncService) SendAsync(id string, msgType string, version string, payload []byte) {
	message := &transport.Message{
		ID:      id,
		MsgType: msgType,
		Version: version,
		Payload: payload,
	}
	s.msgChan <- message
}

// GetVersion returns an empty string if the object doesn't exist or an error occurred.
func (s *SyncService) GetVersion(id string, msgType string) string {
	objectMetadata, err := s.client.GetObjectMetadata(msgType, id)
	if err != nil {
		return ""
	}

	return objectMetadata.Version
}

func (s *SyncService) distributeMessages() {
	for {
		select {
		case <-s.stopChan:
			return
		case msg := <-s.msgChan:
			metaData := client.ObjectMetaData{
				ObjectID:   msg.ID,
				ObjectType: msg.MsgType,
				Version:    msg.Version,
			}

			err := s.client.UpdateObject(&metaData)
			if err != nil {
				s.log.Error(err, "Failed to update the object in the Cloud Sync Service")
				continue
			}

			reader := bytes.NewReader(msg.Payload)
			err = s.client.UpdateObjectData(&metaData, reader)

			if err != nil {
				s.log.Error(err, "Failed to update the object data in the Cloud Sync Service")
				continue
			}

			s.log.Info("Message sent successfully", "id", msg.ID, "type", msg.MsgType, "version",
				msg.Version)
		}
	}
}
