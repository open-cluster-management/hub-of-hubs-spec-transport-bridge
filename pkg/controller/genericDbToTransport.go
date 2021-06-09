package controller

import (
	"encoding/json"
	"fmt"
	dataTypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/hub-of-hubs-transport-bridge/pkg/bundle"
	hohDb "github.com/open-cluster-management/hub-of-hubs-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-transport-bridge/pkg/transport"
	"log"
	"time"
)

const (
	TimeFormat = "2006-01-02_15-04-05"
)

type genericDbToTransport struct {
	db                  hohDb.HubOfHubsDb
	transport           transport.Transport
	dbTableName         string
	transportBundleKey  string
	lastUpdateTimestamp *time.Time
	createObjFunc       bundle.CreateObjectFunction
	createBundleFunc    bundle.CreateBundleFunction
}

func (g *genericDbToTransport) Init() {
	// on initialization, we initialize the lastUpdateTimestamp from the transport layer, as this is the last timestamp
	// that transport bridge sent an update.
	// later, in SyncBundle, it will check the db if there are newer updates and if yes it will send it with
	// transport layer and update the lastUpdateTimestamp field accordingly.
	timestamp := g.initLastUpdateTimestampFromTransport()
	if timestamp != nil {
		g.lastUpdateTimestamp = timestamp
	} else {
		g.lastUpdateTimestamp = &time.Time{}
	}
	log.Println(fmt.Sprintf("initialzed syncer for table %s, last update timestamp found %s", g.dbTableName, g.lastUpdateTimestamp))
	g.SyncBundle()
}

func (g * genericDbToTransport) initLastUpdateTimestampFromTransport() *time.Time {
	version := g.transport.GetVersion(g.transportBundleKey, dataTypes.SpecBundle)
	if version == "" {
		return nil
	}
	timestamp, err := time.Parse(TimeFormat, version)
	if err != nil {
		return nil
	}
	return &timestamp
}

func (g * genericDbToTransport) SyncBundle() {
	lastUpdateTimestamp, err := g.db.GetLastUpdateTimestamp(g.dbTableName)
	if err != nil {
		log.Println(fmt.Sprintf("unable to sync %s bundle to leaf hubs - %s", g.dbTableName, err))
		return
	}
	if !lastUpdateTimestamp.After(*g.lastUpdateTimestamp) { // sync only if something has changed
		return
	}
	// if we got here, then the last update timestamp from db is after what we have in memory.
	// this means something has changed in db, syncing to transport.
	bundleResult := g.createBundleFunc()
	lastUpdateTimestamp, err = g.db.GetBundle(g.dbTableName, g.createObjFunc, bundleResult)
	if err != nil {
		log.Fatalf("unable to sync %s bundle to leaf hubs - %s", g.dbTableName, err)
	}
	g.lastUpdateTimestamp = lastUpdateTimestamp
	g.syncToTransport(g.transportBundleKey, dataTypes.SpecBundle, lastUpdateTimestamp, bundleResult.ToGenericBundle())
}


func (g *genericDbToTransport) syncToTransport(id string, objType string, timestamp *time.Time, payload *dataTypes.ObjectsBundle) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("failed to sync object from type %s with id %s- %s", objType, id, err)
		return
	}
	g.transport.Send(id, objType, timestamp.Format(TimeFormat), payloadBytes)
}