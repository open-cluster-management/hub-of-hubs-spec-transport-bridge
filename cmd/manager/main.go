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
	compressor "github.com/open-cluster-management/hub-of-hubs-message-compression"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/controller"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/db/postgresql"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport/kafka"
	syncservice "github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport/sync-service"
	"github.com/operator-framework/operator-sdk/pkg/log/zap"
	sdkVersion "github.com/operator-framework/operator-sdk/version"
	"github.com/spf13/pflag"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	metricsHost                             = "0.0.0.0"
	metricsPort                       int32 = 8965
	envVarControllerNamespace               = "POD_NAMESPACE"
	envVarTransportMsgCompressionType       = "TRANSPORT_MESSAGE_COMPRESSION_TYPE"
	envVarTransportType                     = "TRANSPORT_TYPE"
	kafkaTransportTypeName                  = "kafka"
	syncServiceTransportTypeName            = "sync-service"
	envVarSyncInterval                      = "SYNC_INTERVAL"
	leaderElectionLockName                  = "hub-of-hubs-spec-transport-bridge-lock"
)

var (
	errEnvVarNotFound     = errors.New("environment variable not found")
	errEnvVarIllegalValue = errors.New("environment variable illegal value")
)

func printVersion(log logr.Logger) {
	log.Info(fmt.Sprintf("Go Version: %s", runtime.Version()))
	log.Info(fmt.Sprintf("Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH))
	log.Info(fmt.Sprintf("Version of operator-sdk: %v", sdkVersion.Version))
}

// function to choose transport type based on env var.
func getTransport(transportType string, transportMsgCompressorType string) (transport.Transport, error) {
	msgCompressor, err := compressor.NewCompressor(compressor.CompressionType(transportMsgCompressorType))
	if err != nil {
		return nil, fmt.Errorf("failed to create message-compressor: %w", err)
	}

	switch transportType {
	case kafkaTransportTypeName:
		kafkaProducer, err := kafka.NewProducer(msgCompressor, ctrl.Log.WithName("kafka"))
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka-producer: %w", err)
		}

		return kafkaProducer, nil
	case syncServiceTransportTypeName:
		syncService, err := syncservice.NewSyncService(msgCompressor, ctrl.Log.WithName("sync-service"))
		if err != nil {
			return nil, fmt.Errorf("failed to create sync-service: %w", err)
		}

		return syncService, nil
	default:
		return nil, fmt.Errorf("%w: %s - %s is not a valid option", errEnvVarIllegalValue, envVarTransportType,
			transportType)
	}
}

func readEnvVars() (string, time.Duration, string, string, error) {
	leaderElectionNamespace, found := os.LookupEnv(envVarControllerNamespace)
	if !found {
		return "", 0, "", "", fmt.Errorf("%w: %s", errEnvVarNotFound, envVarControllerNamespace)
	}

	syncIntervalString, found := os.LookupEnv(envVarSyncInterval)
	if !found {
		return "", 0, "", "", fmt.Errorf("%w: %s", errEnvVarNotFound, envVarSyncInterval)
	}

	syncInterval, err := time.ParseDuration(syncIntervalString)
	if err != nil {
		return "", 0, "", "", fmt.Errorf("the environment var %s is not a valid duration - %w",
			envVarSyncInterval, err)
	}

	transportType, found := os.LookupEnv(envVarTransportType)
	if !found {
		return "", 0, "", "", fmt.Errorf("%w: %s", errEnvVarNotFound, envVarTransportType)
	}

	transportMsgCompressionType, found := os.LookupEnv(envVarTransportMsgCompressionType)
	if !found {
		return "", 0, "", "", fmt.Errorf("%w: %s", errEnvVarNotFound, envVarTransportMsgCompressionType)
	}

	return leaderElectionNamespace, syncInterval, transportType, transportMsgCompressionType, nil
}

// function to handle defers with exit, see https://stackoverflow.com/a/27629493/553720.
func doMain() int {
	pflag.CommandLine.AddFlagSet(zap.FlagSet())
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	ctrl.SetLogger(zap.Logger())
	log := ctrl.Log.WithName("cmd")

	printVersion(log)

	leaderElectionNamespace, syncInterval, transportType, transportMsgCompressionType, err := readEnvVars()
	if err != nil {
		log.Error(err, "initialization error")
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
	transportObj, err := getTransport(transportType, transportMsgCompressionType)
	if err != nil {
		log.Error(err, "transport initialization error")
		return 1
	}

	transportObj.Start()
	defer transportObj.Stop()

	mgr, err := createManager(leaderElectionNamespace, postgreSQL, transportObj, syncInterval)
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

func createManager(leaderElectionNamespace string, postgreSQL db.SpecDB, transportObj transport.Transport,
	syncInterval time.Duration) (ctrl.Manager, error) {
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

	if err := controller.AddDBToTransportSyncers(mgr, postgreSQL, transportObj, syncInterval); err != nil {
		return nil, fmt.Errorf("failed to add db syncers: %w", err)
	}

	return mgr, nil
}

func main() {
	os.Exit(doMain())
}
