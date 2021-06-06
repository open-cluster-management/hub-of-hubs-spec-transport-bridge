package controller

import (
	"encoding/json"
	dataTypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/hub-of-hubs-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-transport-bridge/pkg/transport"
	"log"
	"sync"
	"time"
)

const (
	policiesObjectId          = "Policies"
	placementRulesObjectId    = "PlacementRules"
	placementBindingsObjectId = "PlacementBindings"
	TimeFormat                = "2006-01-02_15-04-05"
)


type HubOfHubsTransportBridge struct {
	db                        db.HubOfHubsDb
	transport                 transport.Transport
	lastPolicyUpdate          *time.Time
	lastPlacementRuleUpdate   *time.Time
	lastPlacementBinginUpdate *time.Time
	periodicSyncInterval      time.Duration
	stopChan                  chan struct{}
	stopOnce                  sync.Once
}

func NewTransportBridge(db db.HubOfHubsDb, transport transport.Transport, syncInterval time.Duration) *HubOfHubsTransportBridge {
	return &HubOfHubsTransportBridge {
		db: db,
		transport: transport,
		periodicSyncInterval: syncInterval,
	}
}

func (b *HubOfHubsTransportBridge) Start() {
	b.syncPolicies()
	b.syncPlacementRules()
	b.syncPlacementBindings()
	b.periodicSync()

}

func (b *HubOfHubsTransportBridge) Stop() {
	b.stopOnce.Do(func() {
		close(b.stopChan)
	})
}

func (b *HubOfHubsTransportBridge) syncPolicies() {
	policiesBundle, lastUpdateTimestamp, err := b.db.GetPoliciesBundle()
	if err != nil {
		log.Fatalf("unable to sync policies to leaf hubs - %s", err)
	}
	b.lastPolicyUpdate = lastUpdateTimestamp
	b.syncObject(policiesObjectId, dataTypes.SpecBundle, lastUpdateTimestamp, policiesBundle.ToGenericBundle())
}

func (b *HubOfHubsTransportBridge) syncPlacementRules() {
	placementRulesBundle, lastUpdateTimestamp, err := b.db.GetPlacementRulesBundle()
	if err != nil {
		log.Fatalf("unable to sync placement rules to leaf hubs - %s", err)
	}
	b.lastPlacementRuleUpdate = lastUpdateTimestamp
	b.syncObject(placementRulesObjectId, dataTypes.SpecBundle, lastUpdateTimestamp, placementRulesBundle.ToGenericBundle())
}

func (b *HubOfHubsTransportBridge) syncPlacementBindings() {
	placementBindingsBundle, lastUpdateTimestamp, err := b.db.GetPlacementBindingsBundle()
	if err != nil {
		log.Fatalf("unable to sync placement bindings to leaf hubs - %s", err)
	}
	b.lastPlacementBinginUpdate = lastUpdateTimestamp
	b.syncObject(placementBindingsObjectId, dataTypes.SpecBundle, lastUpdateTimestamp, placementBindingsBundle.ToGenericBundle())
}

func (b *HubOfHubsTransportBridge) periodicSync() {
	ticker := time.NewTicker(b.periodicSyncInterval)
	for {
		select {
		case <-b.stopChan:
			ticker.Stop()
			return
		case <-ticker.C:
			lastPolicyUpdate, err := b.db.GetLastUpdateTimestamp(db.Policy)
			if err == nil && lastPolicyUpdate.After(*b.lastPolicyUpdate) { // sync if something changed
				b.syncPolicies()
			}
			lastPlacementRuleUpdate, err := b.db.GetLastUpdateTimestamp(db.PlacementRule)
			if err == nil && lastPlacementRuleUpdate.After(*b.lastPlacementRuleUpdate) { // sync if something changed
				b.syncPlacementRules()
			}
			lastPlacementBindingUpdate, err := b.db.GetLastUpdateTimestamp(db.PlacementBinding)
			if err == nil && lastPlacementBindingUpdate.After(*b.lastPlacementBinginUpdate) { // sync if something changed
				b.syncPlacementBindings()
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
