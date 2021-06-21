package controller

import (
	appsv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/apps/v1"
	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sync"
	"time"
)

const (
	policiesMsgKey             = "Policies"
	placementRulesMsgKey       = "PlacementRules"
	placementBindingsMsgKey    = "PlacementBindings"
	policiesTableName          = "policies"
	placementRulesTableName    = "placementrules"
	placementBindingsTableName = "placementbindings"
)

type HubOfHubsTransportBridge struct {
	periodicSyncInterval time.Duration
	dbToTransportSyncers []*genericDBToTransportSyncer
	stopChan             chan struct{}
	startOnce            sync.Once
	stopOnce             sync.Once
}

func NewTransportBridge(db db.HubOfHubsSpecDB, transport transport.Transport,
	syncInterval time.Duration) *HubOfHubsTransportBridge {
	return &HubOfHubsTransportBridge{
		periodicSyncInterval: syncInterval,
		stopChan:             make(chan struct{}, 1),
		dbToTransportSyncers: []*genericDBToTransportSyncer{
			{ // syncer for policy
				db:                 db,
				transport:          transport,
				dbTableName:        policiesTableName,
				transportBundleKey: policiesMsgKey,
				createObjFunc:      func() metav1.Object { return &policiesv1.Policy{} },
				createBundleFunc:   bundle.NewBaseBundle,
			},
			{ // syncer for placement rule
				db:                 db,
				transport:          transport,
				dbTableName:        placementRulesTableName,
				transportBundleKey: placementRulesMsgKey,
				createObjFunc:      func() metav1.Object { return &appsv1.PlacementRule{} },
				createBundleFunc:   bundle.NewBaseBundle,
			},
			{ // syncer for placement binding
				db:                 db,
				transport:          transport,
				dbTableName:        placementBindingsTableName,
				transportBundleKey: placementBindingsMsgKey,
				createObjFunc:      func() metav1.Object { return &policiesv1.PlacementBinding{} },
				createBundleFunc:   bundle.NewPlacementBindingBundle,
			},
		},
	}
}

func (b *HubOfHubsTransportBridge) Start() {
	b.startOnce.Do(func() {
		for _, syncer := range b.dbToTransportSyncers {
			syncer.Init()
		}
		b.periodicSync()
	})
}

func (b *HubOfHubsTransportBridge) Stop() {
	b.stopOnce.Do(func() {
		close(b.stopChan)
	})
}

func (b *HubOfHubsTransportBridge) periodicSync() {
	ticker := time.NewTicker(b.periodicSyncInterval)
	for {
		select {
		case <-b.stopChan:
			ticker.Stop()
			return
		case <-ticker.C:
			for _, syncer := range b.dbToTransportSyncers {
				syncer.SyncBundle()
			}
		}
	}
}
