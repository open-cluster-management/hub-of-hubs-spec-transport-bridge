package postgresql

import (
	"context"
	"fmt"
)

// GetManagedClusterLabelsStatus gets the labels present in managed-cluster CR metadata from a specific table.
func (p *PostgreSQL) GetManagedClusterLabelsStatus(ctx context.Context, tableName string, leafHubName string,
	managedClusterName string) (map[string]string, error) {
	labels := make(map[string]string)

	if err := p.conn.QueryRow(ctx, fmt.Sprintf(`SELECT payload->'metadata'->'labels' FROM status.%s WHERE 
leaf_hub_name=$1 AND payload->'metadata'->>'name'=$2`, tableName), leafHubName,
		managedClusterName).Scan(&labels); err != nil {
		return nil, fmt.Errorf("error reading from table status.%s - %w", tableName, err)
	}

	return labels, nil
}

// GetManagedClusterLeafHubName returns leaf-hub name for a given managed cluster from a specific table.
// TODO: once non-k8s-restapi exposes hub names, remove line.
func (p *PostgreSQL) GetManagedClusterLeafHubName(ctx context.Context, tableName string,
	managedClusterName string) (string, error) {
	var leafHubName string
	if err := p.conn.QueryRow(ctx, fmt.Sprintf(`SELECT leaf_hub_name FROM status.%s WHERE 
		payload->'metadata'->>'name'=$1`, tableName), managedClusterName).Scan(&leafHubName); err != nil {
		return "", fmt.Errorf("error reading from table status.%s - %w", tableName, err)
	}

	return leafHubName, nil
}
