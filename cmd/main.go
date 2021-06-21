package main

import (
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/controller"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db/postgresql"
	hohSyncService "github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport/sync-service"
	"log"
	"os"
	"time"
)

const (
	hohTransportSyncInterval = "HOH_TRANSPORT_SYNC_INTERVAL"
)

func main() {
	// db layer initialization
	postgreSQL := postgresql.NewPostgreSQL()
	defer postgreSQL.Stop()

	// transport layer initialization
	syncServiceObj := hohSyncService.NewSyncService()
	syncServiceObj.Start()
	defer syncServiceObj.Stop()

	syncIntervalStr := os.Getenv(hohTransportSyncInterval)
	if syncIntervalStr == "" {
		log.Fatalf("the expected var %s is not set in environment variables", hohTransportSyncInterval)
	}
	interval, err := time.ParseDuration(syncIntervalStr)
	if err != nil {
		log.Fatalf("the expected var %s is not valid duration", hohTransportSyncInterval)
	}
	transportBridgeController := controller.NewTransportBridge(postgreSQL, syncServiceObj, interval)
	transportBridgeController.Start()
	defer transportBridgeController.Stop()
}
