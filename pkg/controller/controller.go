package controller

import (
	"encoding/json"
	"github.com/open-cluster-management/hub-of-hubs-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-transport-bridge/pkg/transport"
	"log"
	"sync"
	"time"
)

const (
	policyObjectId = "Policy"
	PolicyMessageType = "PoliciesBundle"
	TimeFormat = "2006-01-02_15-04-05"
)


type HubOfHubsTransportBridge struct {
	db 					 	db.HubOfHubsDb
	transport          	 	transport.Transport
	lastPolicyUpdate 		*time.Time
	periodicSyncInterval 	time.Duration
	stopChan             	chan struct{}
	stopOnce             	sync.Once
}

func NewTransportBridge(db db.HubOfHubsDb, transport transport.Transport, syncInterval time.Duration) *HubOfHubsTransportBridge {
	transportBridge := &HubOfHubsTransportBridge{
		db: db,
		transport: transport,
		periodicSyncInterval: syncInterval,
	}
	return transportBridge
}

func (b *HubOfHubsTransportBridge) Start() {
	b.syncPolicies()
	b.periodicSync()

}

func (b *HubOfHubsTransportBridge) Stop() {
	b.stopOnce.Do(func() {
		close(b.stopChan)
	})
}

func (b *HubOfHubsTransportBridge) syncPolicies() {
	policiesBundle, lastPolicyUpdate, err := b.db.GetPoliciesBundle()
	if err != nil {
		log.Fatalf("unable to do initial sync to leaf hubs - %s", err)
	}
	b.lastPolicyUpdate = lastPolicyUpdate
	b.syncObject(policyObjectId, PolicyMessageType, lastPolicyUpdate, policiesBundle)
}

func (b *HubOfHubsTransportBridge) periodicSync() {
	ticker := time.NewTicker(b.periodicSyncInterval)
	for {
		select {
		case <-b.stopChan:
			ticker.Stop()
			return
		case <-ticker.C:
			lastPolicyUpdate, err := b.db.GetPoliciesLastUpdateTimestamp()
			if err != nil {
				log.Printf("error syncing periodically - %s", err)
				continue
			}
			// sync policies only if something has changed
			if lastPolicyUpdate.After(*b.lastPolicyUpdate) {
				b.syncPolicies()
			}
		}
	}
}

func (b *HubOfHubsTransportBridge) syncObject(id string, objType string, timestamp *time.Time, payload interface{}) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("failed to sync object from type %s with id %s- %s", objType, id, err)
		return
	}
	b.transport.Send(id, objType, timestamp.Format(TimeFormat), payloadBytes)
}

