package syncservice

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/go-logr/logr"
	"github.com/open-horizon/edge-sync-service-client/client"
)

const (
	envVarSyncServiceProtocol = "SYNC_SERVICE_PROTOCOL"
	envVarSyncServiceHost     = "SYNC_SERVICE_HOST"
	envVarSyncServicePort     = "SYNC_SERVICE_PORT"
)

// SyncService abstracts Open Horizon Sync Service usage.
type SyncService struct {
	client    *client.SyncServiceClient
	msgChan   chan *syncServiceMessage
	stopChan  chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	log       logr.Logger
}

// NewSyncService returns a new instance of SyncService object.
func NewSyncService(log logr.Logger) (*SyncService, error) {
	serverProtocol, host, port, err := readEnvVars()
	if err != nil {
		return nil, err
	}

	syncServiceClient := client.NewSyncServiceClient(serverProtocol, host, port)

	syncServiceClient.SetOrgID("myorg")
	syncServiceClient.SetAppKeyAndSecret("user@myorg", "")

	return &SyncService{
		client:   syncServiceClient,
		msgChan:  make(chan *syncServiceMessage),
		stopChan: make(chan struct{}, 1),
		log:      log,
	}, nil
}

func readEnvVars() (string, string, uint16, error) {
	protocol, found := os.LookupEnv(envVarSyncServiceProtocol)
	if !found {
		return "", "", 0, fmt.Errorf("not found: environment variable %s", envVarSyncServiceProtocol)
	}

	host, found := os.LookupEnv(envVarSyncServiceHost)
	if !found {
		return "", "", 0, fmt.Errorf("not found: environment variable %s", envVarSyncServiceHost)
	}

	portString, found := os.LookupEnv(envVarSyncServicePort)
	if !found {
		return "", "", 0, fmt.Errorf("not found: environment variable %s", envVarSyncServicePort)
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
	message := &syncServiceMessage{
		id:      id,
		msgType: msgType,
		version: version,
		payload: payload,
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
				ObjectID:   msg.id,
				ObjectType: msg.msgType,
				Version:    msg.version,
			}

			err := s.client.UpdateObject(&metaData)
			if err != nil {
				s.log.Error(err, "Failed to update the object in the Cloud Sync Service")
				continue
			}

			reader := bytes.NewReader(msg.payload)
			err = s.client.UpdateObjectData(&metaData, reader)

			if err != nil {
				s.log.Error(err, "Failed to update the object data in the Cloud Sync Service")
				continue
			}

			s.log.Info("Message sent successfully", "id", msg.id, "type", msg.msgType, "version",
				msg.version)
		}
	}
}
