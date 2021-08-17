// Copyright (c) 2021 Red Hat, Inc.
// Copyright Contributors to the Open Cluster Management project

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/controller"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db/postgresql"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	kafka "github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport/kafka-client"
	hohSyncService "github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport/sync-service"
	"github.com/operator-framework/operator-sdk/pkg/log/zap"
	sdkVersion "github.com/operator-framework/operator-sdk/version"
	"github.com/spf13/pflag"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	metricsHost                        = "0.0.0.0"
	metricsPort                  int32 = 8965
	kafkaTransportTypeName             = "kafka"
	syncServiceTransportTypeName       = "syncservice"
	envVarControllerNamespace          = "POD_NAMESPACE"
	envVarTransportSyncInterval        = "TRANSPORT_SYNC_INTERVAL"
	envVarTransportType                = "TRANSPORT_TYPE"
	leaderElectionLockName             = "hub-of-hubs-spec-transport-bridge-lock"
)

var (
	errEnvVarNotFound     = errors.New("not found environment variable")
	errEnvVarIllegalValue = errors.New("environment variable illegal value")
)

func printVersion(log logr.Logger) {
	log.Info(fmt.Sprintf("Go Version: %s", runtime.Version()))
	log.Info(fmt.Sprintf("Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH))
	log.Info(fmt.Sprintf("Version of operator-sdk: %v", sdkVersion.Version))
}

// function to choose transport type based on env var.
func getTransport(transportType string) (transport.Transport, error) {
	switch transportType {
	case kafkaTransportTypeName:
		kafkaProducer, err := kafka.NewProducer(ctrl.Log.WithName("kafka-client"))
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka-producer: %w", err)
		}

		return kafkaProducer, nil
	case syncServiceTransportTypeName:
		syncService, err := hohSyncService.NewSyncService(ctrl.Log.WithName("sync-service"))
		if err != nil {
			return nil, fmt.Errorf("failed to create sync-service: %w", err)
		}

		return syncService, nil
	default:
		return nil, errEnvVarIllegalValue
	}
}

func readEnvVars(log logr.Logger) (string, time.Duration, string, error) {
	leaderElectionNamespace, found := os.LookupEnv(envVarControllerNamespace)
	if !found {
		log.Error(nil, "Not found:", "environment variable", envVarControllerNamespace)
		return "", 0, "", errEnvVarNotFound
	}

	syncIntervalString, found := os.LookupEnv(envVarTransportSyncInterval)
	if !found {
		log.Error(nil, "Not found:", "environment variable", envVarTransportSyncInterval)
		return "", 0, "", errEnvVarNotFound
	}

	syncInterval, err := time.ParseDuration(syncIntervalString)
	if err != nil {
		log.Error(err, "the environment var ", envVarTransportSyncInterval, " is not valid duration")
		return "", 0, "", errEnvVarNotFound
	}

	transportType, found := os.LookupEnv(envVarTransportType)
	if !found {
		log.Error(nil, "Not found:", "environment variable", envVarTransportType)
		return "", 0, "", errEnvVarNotFound
	}

	return leaderElectionNamespace, syncInterval, transportType, nil
}

// function to handle defers with exit, see https://stackoverflow.com/a/27629493/553720.
func doMain() int {
	pflag.CommandLine.AddFlagSet(zap.FlagSet())
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	ctrl.SetLogger(zap.Logger())
	log := ctrl.Log.WithName("cmd")

	printVersion(log)

	leaderElectionNamespace, syncInterval, transportType, err := readEnvVars(log)
	if err != nil {
		return 1
	}
	// db layer initialization
	postgreSQL, err := postgresql.NewPostgreSQL()
	if err != nil {
		log.Error(err, "initialization error", "failed to initialize", "PostgreSQL")
		return 1
	}

	defer postgreSQL.Stop()

	// transport layer initialization
	transportObj, err := getTransport(transportType)
	if err != nil {
		log.Error(err, "initialization error", "failed to initialize", transportType)
		return 1
	}

	transportObj.Start()
	defer transportObj.Stop()

	mgr, err := createManager(leaderElectionNamespace, metricsHost, metricsPort, postgreSQL, transportObj, syncInterval)
	if err != nil {
		log.Error(err, "Failed to create manager")
		return 1
	}

	log.Info("Starting the Cmd.")

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "Manager exited non-zero")
		return 1
	}

	return 0
}

func createManager(leaderElectionNamespace, metricsHost string, metricsPort int32, postgreSQL db.HubOfHubsSpecDB,
	transport transport.Transport, syncInterval time.Duration) (ctrl.Manager, error) {
	options := ctrl.Options{
		MetricsBindAddress:      fmt.Sprintf("%s:%d", metricsHost, metricsPort),
		LeaderElection:          true,
		LeaderElectionID:        leaderElectionLockName,
		LeaderElectionNamespace: leaderElectionNamespace,
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), options)
	if err != nil {
		return nil, fmt.Errorf("failed to create a new manager: %w", err)
	}

	if err := controller.AddDBToTransportSyncers(mgr, postgreSQL, transport, syncInterval); err != nil {
		return nil, fmt.Errorf("failed to add db syncers: %w", err)
	}

	return mgr, nil
}

func main() {
	os.Exit(doMain())
}
