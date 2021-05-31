package postgresql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
	dataTypes "github.com/open-cluster-management/hub-of-hubs-transport-bridge/pkg/data-types"
	"log"
	"os"
	"time"
)

const (
	databaseUrlEnvVar = "DATABASE_URL"
	policiesTableName = "spec.policies"
)

type PostgreSql struct {
	conn *pgxpool.Pool
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
	postgreSql := &PostgreSql{
		conn: dbConnectionPool,
	}
	return postgreSql
}

func (db *PostgreSql) Stop() {
	db.conn.Close()
}


func (db *PostgreSql) queryPolicy(id string) (*policiesv1.Policy, error) {
	policy := &policiesv1.Policy{}
	err := db.conn.QueryRow(context.Background(),
		`SELECT payload FROM spec.policies WHERE id = $1`, id).Scan(&policy)

	if err == pgx.ErrNoRows {
		return nil, errors.New(fmt.Sprintf("Policy with id %s doesn't exist", id))
	}
	return policy, nil
}

func (db *PostgreSql) GetPoliciesBundle() (*dataTypes.PoliciesBundle, *time.Time, error) {
	timestamp, err := db.GetPoliciesLastUpdateTimestamp()
	if err != nil {
		return nil, nil, err
	}
	rows, _ := db.conn.Query(context.Background(),
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

func (db *PostgreSql) GetPoliciesLastUpdateTimestamp() (*time.Time, error) {
	var lastTimestamp time.Time
	err := db.conn.QueryRow(context.Background(),
		`SELECT MAX(updated_at) FROM spec.policies`).Scan(&lastTimestamp)

	if err == pgx.ErrNoRows {
		return nil, errors.New(fmt.Sprintf("no policies in the table"))
	}
	return &lastTimestamp, nil
}