package postgresql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/open-cluster-management/hub-of-hubs-transport-bridge/pkg/bundle"
	"log"
	"os"
	"time"
)

const (
	databaseURLEnvVar = "DATABASE_URL"
)

type PostgreSQL struct {
	conn *pgxpool.Pool
}

func NewPostgreSQL() *PostgreSQL {
	databaseURL := os.Getenv(databaseURLEnvVar)
	if databaseURL == "" {
		log.Fatalf("the expected argument %s is not set in environment variables", databaseURLEnvVar)
	}
	dbConnectionPool, err := pgxpool.Connect(context.Background(), databaseURL)
	if err != nil {
		log.Fatalf("unable to connect to db: %s", err)
	}
	return &PostgreSQL{
		conn: dbConnectionPool,
	}
}

func (p *PostgreSQL) Stop() {
	p.conn.Close()
}

func (p *PostgreSQL) GetBundle(tableName string, createObjFunc bundle.CreateObjectFunction,
	intoBundle bundle.Bundle) (*time.Time, error) {
	timestamp, err := p.GetLastUpdateTimestamp(tableName)
	if err != nil {
		return nil, err
	}
	rows, _ := p.conn.Query(context.Background(),
		fmt.Sprintf(`SELECT id,payload,deleted FROM spec.%s`, tableName))
	for rows.Next() {
		var id string
		var deleted bool
		object := createObjFunc()
		err := rows.Scan(&id, &object, &deleted)
		if err != nil {
			log.Printf("error reading from table spec.%s - %s", tableName, err)
			return nil, err
		}
		if deleted {
			intoBundle.AddDeletedObject(object)
		} else {
			intoBundle.AddObject(object)
		}
	}
	return timestamp, nil
}

func (p *PostgreSQL) GetLastUpdateTimestamp(tableName string) (*time.Time, error) {
	var lastTimestamp time.Time
	err := p.conn.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT MAX(updated_at) FROM spec.%s`, tableName)).Scan(&lastTimestamp)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("no objects in the table spec.%s", tableName)
	}
	return &lastTimestamp, nil
}
