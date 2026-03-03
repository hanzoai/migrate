// Package datastore implements the database.Driver interface for Hanzo Datastore
// (a ClickHouse-compatible database engine). It registers the "datastore://" URL
// scheme so that golang-migrate can connect to Datastore instances natively.
//
// Usage:
//
//	import _ "github.com/golang-migrate/migrate/v4/database/datastore"
//
// Then use a URL like:
//
//	datastore://host:9000?username=default&database=default&x-multi-statement=true
package datastore

import (
	"database/sql"
	"net/url"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"

	_ "github.com/ClickHouse/clickhouse-go"
)

func init() {
	database.Register("datastore", &Datastore{})
}

// Datastore wraps the ClickHouse driver, registering under the "datastore" scheme.
type Datastore struct {
	clickhouse.ClickHouse
}

// Open rewrites the "datastore://" scheme to "clickhouse://" and delegates
// to the underlying ClickHouse driver.
func (d *Datastore) Open(dsn string) (database.Driver, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	u.Scheme = "clickhouse"
	return d.ClickHouse.Open(u.String())
}

// WithInstance creates a new Datastore driver from an existing sql.DB connection.
func WithInstance(conn *sql.DB, config *clickhouse.Config) (database.Driver, error) {
	return clickhouse.WithInstance(conn, config)
}
