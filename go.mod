module github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge

go 1.16

require (
	github.com/go-logr/logr v0.3.0
	github.com/jackc/pgx/v4 v4.11.0
	github.com/open-cluster-management/governance-policy-propagator v0.0.0-20210520203318-a78632de1e26
	github.com/open-cluster-management/hub-of-hubs-data-types v0.1.0
	github.com/open-cluster-management/multicloud-operators-channel v1.0.1-0.20201120143200-e505a259de45
	github.com/open-cluster-management/multicloud-operators-subscription v1.2.2-2-20201130-59f96
	github.com/open-horizon/edge-sync-service-client v0.0.0-20190711093406-dc3a19905da2
	github.com/open-horizon/edge-utilities v0.0.0-20190711093331-0908b45a7152 // indirect
	github.com/operator-framework/operator-sdk v0.19.4
	github.com/spf13/pflag v1.0.5
	k8s.io/apimachinery v0.20.5
	k8s.io/client-go v12.0.0+incompatible
	sigs.k8s.io/application v0.8.3
	sigs.k8s.io/controller-runtime v0.6.3
)

replace k8s.io/client-go => k8s.io/client-go v0.20.5
