package postgresql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	appsv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/apps/v1"
	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
	dataTypes "github.com/open-cluster-management/hub-of-hubs-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-transport-bridge/pkg/db"
	"log"
	"os"
	"time"
)

const (
	databaseUrlEnvVar = "DATABASE_URL"
	policiesTableName = "spec.policies"
	placementRulesTableName = "spec.placementrules"
	placementBindingsTableName = "spec.placementbindings"
)

type PostgreSql struct {
	conn *pgxpool.Pool
	acmType2TableName map[db.AcmType]string
}

func NewPostgreSql() *PostgreSql {
	databaseUrl := os.Getenv(databaseUrlEnvVar)
	if databaseUrl == "" {
		log.Fatalf("the expected argument %s is not set in environment variables", databaseUrlEnvVar)
	}
	dbConnectionPool, err := pgxpool.Connect(context.Background(), databaseUrl)
	if err != nil {
		log.Fatalf("unable to connect to db: %s", err)
	}
	acmType2TableName := make(map[db.AcmType]string)
	acmType2TableName[db.Policy] = policiesTableName
	acmType2TableName[db.PlacementRule] = placementRulesTableName
	acmType2TableName[db.PlacementBinding] = placementBindingsTableName
	return &PostgreSql {
		conn: dbConnectionPool,
		acmType2TableName: acmType2TableName,
	}
}

func (p *PostgreSql) Stop() {
	p.conn.Close()
}

func (p *PostgreSql) GetPoliciesBundle() (*dataTypes.PoliciesBundle, *time.Time, error) {
	timestamp, err := p.GetLastUpdateTimestamp(db.Policy)
	if err != nil {
		return nil, nil, err
	}
	rows, _ := p.conn.Query(context.Background(),
		fmt.Sprintf(`SELECT id,payload,deleted FROM %s`, policiesTableName))
	policiesBundle := &dataTypes.PoliciesBundle {
		Policies: make([]*policiesv1.Policy, 0),
		DeletedPolicies: make([]*policiesv1.Policy, 0),
	}
	for rows.Next() {
		var id string
		var deleted bool
		policy := &policiesv1.Policy{}
		err := rows.Scan(&id, &policy, &deleted)
		if err != nil {
			log.Printf("error reading from table %s - %s", policiesTableName, err)
			return nil, nil, err
		}
		if deleted {
			policiesBundle.AddDeletedPolicy(policy)
		} else {
			policiesBundle.AddPolicy(policy)
		}
	}
	return policiesBundle, timestamp, nil
}

func (p *PostgreSql) GetPlacementRulesBundle() (*dataTypes.PlacementRulesBundle, *time.Time, error) {
	timestamp, err := p.GetLastUpdateTimestamp(db.PlacementRule)
	if err != nil {
		return nil, nil, err
	}
	rows, _ := p.conn.Query(context.Background(),
		fmt.Sprintf(`SELECT id,payload,deleted FROM %s`, placementRulesTableName))
	placementRulesBundle := &dataTypes.PlacementRulesBundle {
		PlacementRules: make([]*appsv1.PlacementRule, 0),
		DeletedPlacementRules: make([]*appsv1.PlacementRule, 0),
	}
	for rows.Next() {
		var id string
		var deleted bool
		placementRule := &appsv1.PlacementRule{}
		err := rows.Scan(&id, &placementRule, &deleted)
		if err != nil {
			log.Printf("error reading from table %s - %s", placementRulesTableName, err)
			return nil, nil, err
		}
		if deleted {
			placementRulesBundle.AddDeletedPlacementRule(placementRule)
		} else {
			placementRulesBundle.AddPlacementRule(placementRule)
		}
	}
	return placementRulesBundle, timestamp, nil
}

func (p *PostgreSql) GetPlacementBindingsBundle() (*dataTypes.PlacementBindingsBundle, *time.Time, error) {
	timestamp, err := p.GetLastUpdateTimestamp(db.PlacementBinding)
	if err != nil {
		return nil, nil, err
	}
	rows, _ := p.conn.Query(context.Background(),
		fmt.Sprintf(`SELECT id,payload,deleted FROM %s`, placementBindingsTableName))
	placementBindingsBundle := &dataTypes.PlacementBindingsBundle {
		PlacementBindings: make([]*policiesv1.PlacementBinding, 0),
		DeletedPlacementBindings: make([]*policiesv1.PlacementBinding, 0),
	}
	for rows.Next() {
		var id string
		var deleted bool
		placementBinding := &policiesv1.PlacementBinding{}
		err := rows.Scan(&id, &placementBinding, &deleted)
		if err != nil {
			log.Printf("error reading from table %s - %s", placementBindingsTableName, err)
			return nil, nil, err
		}
		if deleted {
			placementBindingsBundle.AddDeletedPlacementBinding(placementBinding)
		} else {
			placementBindingsBundle.AddPlacementBinding(placementBinding)
		}
	}
	return placementBindingsBundle, timestamp, nil
}

func (p *PostgreSql) GetLastUpdateTimestamp(acmType db.AcmType) (*time.Time, error) {
	var lastTimestamp time.Time
	err := p.conn.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT MAX(updated_at) FROM %s`, p.acmType2TableName[acmType])).Scan(&lastTimestamp)

	if err == pgx.ErrNoRows {
		return nil, errors.New(fmt.Sprintf("no objects in the table"))
	}
	return &lastTimestamp, nil
}