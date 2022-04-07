package postgresql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stolostron/hub-of-hubs-data-types/bundle/spec"
	"github.com/stolostron/hub-of-hubs-spec-transport-bridge/pkg/bundle"
)

const (
	envVarDatabaseURL = "DATABASE_URL"
)

var errEnvVarNotFound = errors.New("not found environment variable")

// PostgreSQL abstracts PostgreSQL client.
type PostgreSQL struct {
	conn *pgxpool.Pool
}

// NewPostgreSQL creates a new instance of PostgreSQL object.
func NewPostgreSQL() (*PostgreSQL, error) {
	databaseURL, found := os.LookupEnv(envVarDatabaseURL)
	if !found {
		return nil, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarDatabaseURL)
	}

	dbConnectionPool, err := pgxpool.Connect(context.Background(), databaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to db: %w", err)
	}

	return &PostgreSQL{conn: dbConnectionPool}, nil
}

// Stop stops PostgreSQL and closes the connection pool.
func (p *PostgreSQL) Stop() {
	p.conn.Close()
}

// GetLastUpdateTimestamp returns the last update timestamp of a specific table.
func (p *PostgreSQL) GetLastUpdateTimestamp(ctx context.Context, tableName string,
	filterLocalResources bool) (*time.Time, error) {
	var lastTimestamp time.Time

	query := fmt.Sprintf(`SELECT MAX(updated_at) FROM spec.%s WHERE
		payload->'metadata'->'annotations'->'hub-of-hubs.open-cluster-management.io/local-resource' IS NULL`,
		tableName)

	if !filterLocalResources {
		query = fmt.Sprintf(`SELECT MAX(updated_at) FROM spec.%s`, tableName)
	}

	err := p.conn.QueryRow(ctx, query).Scan(&lastTimestamp)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("no objects in the table spec.%s - %w", tableName, err)
	}

	return &lastTimestamp, nil
}

// GetObjectsBundle returns a bundle of objects from a specific table.
func (p *PostgreSQL) GetObjectsBundle(ctx context.Context, tableName string, createObjFunc bundle.CreateObjectFunction,
	intoBundle bundle.ObjectsBundle) (*time.Time, error) {
	timestamp, err := p.GetLastUpdateTimestamp(ctx, tableName, true)
	if err != nil {
		return nil, err
	}

	rows, err := p.conn.Query(ctx, fmt.Sprintf(`SELECT id,payload,deleted FROM spec.%s WHERE
		payload->'metadata'->'annotations'->'hub-of-hubs.open-cluster-management.io/local-resource' IS NULL`,
		tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to query table spec.%s - %w", tableName, err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			objID   string
			deleted bool
		)

		object := createObjFunc()
		if err := rows.Scan(&objID, &object, &deleted); err != nil {
			return nil, fmt.Errorf("error reading from table spec.%s - %w", tableName, err)
		}

		if deleted {
			intoBundle.AddDeletedObject(object)
		} else {
			intoBundle.AddObject(object, objID)
		}
	}

	return timestamp, nil
}

// GetUpdatedManagedClusterLabelsBundles returns a map of leaf-hub -> ManagedClusterLabelsSpecBundle of objects
// belonging to a leaf-hub that had at least once update since the given timestamp, from a specific table.
func (p *PostgreSQL) GetUpdatedManagedClusterLabelsBundles(ctx context.Context, tableName string,
	timestamp *time.Time) (map[string]*spec.ManagedClusterLabelsSpecBundle, error) {
	// select ManagedClusterLabelsSpec entries information from DB
	rows, err := p.conn.Query(ctx, fmt.Sprintf(`SELECT leaf_hub_name,managed_cluster_name,labels,
		deleted_label_keys,updated_at,version FROM spec.%s WHERE leaf_hub_name IN (SELECT DISTINCT(leaf_hub_name) 
		from spec.%s WHERE updated_at::timestamp > timestamp '%s') AND leaf_hub_name != ""`, tableName, tableName,
		timestamp.Format(time.RFC3339Nano)))
	if err != nil {
		return nil, fmt.Errorf("failed to query table spec.%s - %w", tableName, err)
	}

	defer rows.Close()

	leafHubToLabelsSpecBundleMap, err := p.getLabelsSpecBundlesFromRows(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to get managed cluster labels bundles - %w", err)
	}

	return leafHubToLabelsSpecBundleMap, nil
}

func (p *PostgreSQL) getLabelsSpecBundlesFromRows(rows pgx.Rows) (map[string]*spec.ManagedClusterLabelsSpecBundle,
	error) {
	leafHubToLabelsSpecBundleMap := make(map[string]*spec.ManagedClusterLabelsSpecBundle)

	for rows.Next() {
		var (
			leafHubName        string
			managedClusterName string
			labels             map[string]string
			deletedLabelKeys   []string
			updatedAt          time.Time
			version            int64
		)

		if err := rows.Scan(&leafHubName, &managedClusterName, &labels, &deletedLabelKeys, &updatedAt,
			&version); err != nil {
			return nil, fmt.Errorf("error reading from table - %w", err)
		}

		// create ManagedClusterLabelsSpecBundle if not mapped for leafHub
		managedClusterLabelsSpecBundle, found := leafHubToLabelsSpecBundleMap[leafHubName]
		if !found {
			managedClusterLabelsSpecBundle = &spec.ManagedClusterLabelsSpecBundle{
				Objects:     []*spec.ManagedClusterLabelsSpec{},
				LeafHubName: leafHubName,
			}

			leafHubToLabelsSpecBundleMap[leafHubName] = managedClusterLabelsSpecBundle
		}

		// append entry to bundle
		managedClusterLabelsSpecBundle.Objects = append(managedClusterLabelsSpecBundle.Objects,
			&spec.ManagedClusterLabelsSpec{
				ClusterName:      managedClusterName,
				Labels:           labels,
				DeletedLabelKeys: deletedLabelKeys,
				UpdateTimestamp:  updatedAt,
				Version:          version,
			})
	}

	return leafHubToLabelsSpecBundleMap, nil
}
