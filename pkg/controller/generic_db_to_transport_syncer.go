package controller

import (
	"encoding/json"
	"fmt"
	"github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/bundle"
	hohDb "github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	"log"
	"time"
)

const (
	TimeFormat = "2006-01-02_15-04-05.000000"
)

type genericDBToTransportSyncer struct {
	db                  hohDb.HubOfHubsSpecDB
	transport           transport.Transport
	dbTableName         string
	transportBundleKey  string
	lastUpdateTimestamp *time.Time
	createObjFunc       bundle.CreateObjectFunction
	createBundleFunc    bundle.CreateBundleFunction
}

func (g *genericDBToTransportSyncer) Init() {
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
	log.Println(fmt.Sprintf("initialzed syncer for table spec.%s", g.dbTableName))
	g.SyncBundle()
}

func (g *genericDBToTransportSyncer) initLastUpdateTimestampFromTransport() *time.Time {
	version := g.transport.GetVersion(g.transportBundleKey, datatypes.SpecBundle)
	if version == "" {
		return nil
	}
	timestamp, err := time.Parse(TimeFormat, version)
	if err != nil {
		return nil
	}
	return &timestamp
}

func (g *genericDBToTransportSyncer) SyncBundle() {
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
		log.Println(fmt.Sprintf("unable to sync %s bundle to leaf hubs - %s", g.dbTableName, err))
		return
	}
	g.lastUpdateTimestamp = lastUpdateTimestamp
	g.syncToTransport(g.transportBundleKey, datatypes.SpecBundle, lastUpdateTimestamp, bundleResult)
}

func (g *genericDBToTransportSyncer) syncToTransport(id string, objType string, timestamp *time.Time,
	payload bundle.Bundle) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("failed to sync object from type %s with id %s- %s", objType, id, err)
		return
	}
	g.transport.SendAsync(id, objType, timestamp.Format(TimeFormat), payloadBytes)
}
