//go:build datastore

package cli

import (
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/golang-migrate/migrate/v4/database/datastore"
)
