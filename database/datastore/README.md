# Datastore

`datastore://host:port?username=user&password=password&database=clicks&x-multi-statement=true`

Hanzo Datastore is a ClickHouse-wire-compatible database engine. This driver
connects through the native `github.com/hanzo-ds/go` driver, which registers the
`database/sql` driver name `datastore`.

| URL Query  | Description |
|------------|-------------|
| `x-migrations-table`| Name of the migrations table |
| `x-migrations-table-engine`| Engine to use for the migrations table, defaults to TinyLog |
| `x-cluster-name` | Name of cluster for creating `schema_migrations` table cluster wide |
| `x-multi-statement` | Enable multiple statements to be ran in a single migration (See note below) |
| `x-multi-statement-max-size` | Maximum size of a single migration in bytes when `x-multi-statement` is set (defaults to 10 MB) |
| `database` | The name of the database to connect to |
| `username` | The user to sign in as |
| `password` | The user's password |
| `host` | The host to connect to. |
| `port` | The port to bind to. |

## Notes

* The Datastore driver does not natively support executing multiple statements in a single query. To allow for multiple statements in a single migration, you can use the `x-multi-statement` param. There are two important caveats:
  * This mode splits the migration text into separately-executed statements by a semi-colon `;`. Thus `x-multi-statement` cannot be used when a statement in the migration contains a string with a semi-colon.
  * The queries are not executed in any sort of transaction/batch, meaning you are responsible for fixing partial migrations.
* Using the default TinyLog table engine for the `schema_migrations` table prevents backing up the table. If you need to back up the database, run the migrations with `x-migrations-table-engine=MergeTree`.
* Datastore cluster mode is not officially supported, but you can try enabling `schema_migrations` table replication by specifying a `x-cluster-name`:
  * When `x-cluster-name` is specified, `x-migrations-table-engine` also should be specified.
  * When `x-cluster-name` is specified, only the `schema_migrations` table is replicated across the cluster. You still need to write your migrations so that the application tables are replicated within the cluster.
* If you want to create a database inside a migration, note that the `schema_migrations` table will live in the `default` database, so you cannot use `USE <database_name>` inside a migration. In this case you may omit the database in the connection string (example [here](examples/migrations/003_create_database.up.sql)).
