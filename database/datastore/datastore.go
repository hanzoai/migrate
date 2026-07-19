// Package datastore implements the golang-migrate database.Driver interface for
// Hanzo Datastore (a ClickHouse-wire-compatible database engine). It registers
// the "datastore://" URL scheme and connects through the native
// github.com/hanzo-ds/go driver, which registers the database/sql driver name
// "datastore".
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
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"

	// Registers the "datastore" database/sql driver (ClickHouse-wire native).
	_ "github.com/hanzo-ds/go"
)

var (
	multiStmtDelimiter = []byte(";")

	// DriverName is the database/sql driver registered by github.com/hanzo-ds/go.
	DriverName = "datastore"

	DefaultMigrationsTable       = "schema_migrations"
	DefaultMigrationsTableEngine = "TinyLog"
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB

	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	DatabaseName          string
	ClusterName           string
	MigrationsTable       string
	MigrationsTableEngine string
	MultiStatementEnabled bool
	MultiStatementMaxSize int
}

func init() {
	database.Register("datastore", &Datastore{})
}

// WithInstance creates a Datastore driver from an existing *sql.DB opened
// against the "datastore" driver.
func WithInstance(conn *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	ds := &Datastore{
		conn:   conn,
		config: config,
	}

	if err := ds.init(); err != nil {
		return nil, err
	}

	return ds, nil
}

// Datastore is a golang-migrate database.Driver backed by the native
// github.com/hanzo-ds/go ClickHouse-wire driver.
type Datastore struct {
	conn     *sql.DB
	config   *Config
	isLocked atomic.Bool
}

func (ds *Datastore) Open(dsn string) (database.Driver, error) {
	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	q := migrate.FilterCustomQuery(purl)
	q.Scheme = DriverName
	conn, err := sql.Open(DriverName, q.String())
	if err != nil {
		return nil, err
	}

	multiStatementMaxSize := DefaultMultiStatementMaxSize
	if s := purl.Query().Get("x-multi-statement-max-size"); len(s) > 0 {
		multiStatementMaxSize, err = strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
	}

	migrationsTableEngine := DefaultMigrationsTableEngine
	if s := purl.Query().Get("x-migrations-table-engine"); len(s) > 0 {
		migrationsTableEngine = s
	}

	ds = &Datastore{
		conn: conn,
		config: &Config{
			MigrationsTable:       purl.Query().Get("x-migrations-table"),
			MigrationsTableEngine: migrationsTableEngine,
			DatabaseName:          purl.Query().Get("database"),
			ClusterName:           purl.Query().Get("x-cluster-name"),
			MultiStatementEnabled: purl.Query().Get("x-multi-statement") == "true",
			MultiStatementMaxSize: multiStatementMaxSize,
		},
	}

	if err := ds.init(); err != nil {
		return nil, err
	}

	return ds, nil
}

func (ds *Datastore) init() error {
	if len(ds.config.DatabaseName) == 0 {
		if err := ds.conn.QueryRow("SELECT currentDatabase()").Scan(&ds.config.DatabaseName); err != nil {
			return err
		}
	}

	if len(ds.config.MigrationsTable) == 0 {
		ds.config.MigrationsTable = DefaultMigrationsTable
	}

	if ds.config.MultiStatementMaxSize <= 0 {
		ds.config.MultiStatementMaxSize = DefaultMultiStatementMaxSize
	}

	if len(ds.config.MigrationsTableEngine) == 0 {
		ds.config.MigrationsTableEngine = DefaultMigrationsTableEngine
	}

	return ds.ensureVersionTable()
}

func (ds *Datastore) Run(r io.Reader) error {
	if ds.config.MultiStatementEnabled {
		var err error
		if e := multistmt.Parse(r, multiStmtDelimiter, ds.config.MultiStatementMaxSize, func(m []byte) bool {
			tq := strings.TrimSpace(string(m))
			if tq == "" {
				return true
			}
			if _, e := ds.conn.Exec(string(m)); e != nil {
				err = database.Error{OrigErr: e, Err: "migration failed", Query: m}
				return false
			}
			return true
		}); e != nil {
			return e
		}
		return err
	}

	migration, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	if _, err := ds.conn.Exec(string(migration)); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migration}
	}

	return nil
}

func (ds *Datastore) Version() (int, bool, error) {
	var (
		version int
		dirty   uint8
		query   = "SELECT version, dirty FROM `" + ds.config.MigrationsTable + "` ORDER BY sequence DESC LIMIT 1"
	)
	if err := ds.conn.QueryRow(query).Scan(&version, &dirty); err != nil {
		if err == sql.ErrNoRows {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return version, dirty == 1, nil
}

func (ds *Datastore) SetVersion(version int, dirty bool) error {
	var (
		bool = func(v bool) uint8 {
			if v {
				return 1
			}
			return 0
		}
		tx, err = ds.conn.Begin()
	)
	if err != nil {
		return err
	}

	query := "INSERT INTO " + ds.config.MigrationsTable + " (version, dirty, sequence) VALUES (?, ?, ?)"
	if _, err := tx.Exec(query, version, bool(dirty), time.Now().UnixNano()); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return tx.Commit()
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Datastore type.
func (ds *Datastore) ensureVersionTable() (err error) {
	if err = ds.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := ds.Unlock(); e != nil {
			err = errors.Join(err, e)
		}
	}()

	var (
		table string
		query = "SHOW TABLES FROM " + quoteIdentifier(ds.config.DatabaseName) + " LIKE '" + ds.config.MigrationsTable + "'"
	)
	// check if migration table exists
	if err := ds.conn.QueryRow(query).Scan(&table); err != nil {
		if err != sql.ErrNoRows {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	} else {
		return nil
	}

	// if not, create the empty migration table
	if len(ds.config.ClusterName) > 0 {
		query = fmt.Sprintf(`
			CREATE TABLE %s ON CLUSTER %s (
				version    Int64,
				dirty      UInt8,
				sequence   UInt64
			) Engine=%s`, ds.config.MigrationsTable, ds.config.ClusterName, ds.config.MigrationsTableEngine)
	} else {
		query = fmt.Sprintf(`
			CREATE TABLE %s (
				version    Int64,
				dirty      UInt8,
				sequence   UInt64
			) Engine=%s`, ds.config.MigrationsTable, ds.config.MigrationsTableEngine)
	}

	if strings.HasSuffix(ds.config.MigrationsTableEngine, "Tree") {
		query = fmt.Sprintf(`%s ORDER BY sequence`, query)
	}

	if _, err := ds.conn.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (ds *Datastore) Drop() (err error) {
	query := "SHOW TABLES FROM " + quoteIdentifier(ds.config.DatabaseName)
	tables, err := ds.conn.Query(query)

	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = errors.Join(err, errClose)
		}
	}()

	for tables.Next() {
		var table string
		if err := tables.Scan(&table); err != nil {
			return err
		}

		query = "DROP TABLE IF EXISTS " + quoteIdentifier(ds.config.DatabaseName) + "." + quoteIdentifier(table)

		if _, err := ds.conn.Exec(query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}
	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func (ds *Datastore) Lock() error {
	if !ds.isLocked.CompareAndSwap(false, true) {
		return database.ErrLocked
	}

	return nil
}

func (ds *Datastore) Unlock() error {
	if !ds.isLocked.CompareAndSwap(true, false) {
		return database.ErrNotLocked
	}

	return nil
}

func (ds *Datastore) Close() error { return ds.conn.Close() }

// Copied from lib/pq implementation: https://github.com/lib/pq/blob/v1.9.0/conn.go#L1611
func quoteIdentifier(name string) string {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
