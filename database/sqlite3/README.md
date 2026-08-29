# sqlite3

`sqlite3://path/to/database?query`

Unlike other migrate database drivers, the sqlite3 driver will automatically wrap each migration in an implicit transaction by default.  Migrations must not contain explicit `BEGIN` or `COMMIT` statements.  This behavior may change in a future major release.  (See below for a workaround.)

The sqlite3 database driver accepts the [connection-string parameters](https://github.com/mattn/go-sqlite3/blob/master/README.md#connection-string) of the driver `github.com/hanzoai/csqlite` forks.  The auxiliary query parameters listed below may be supplied to tailor migrate behavior.  All auxiliary query parameters are optional.

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table.  Defaults to `schema_migrations`. |
| `x-no-tx-wrap` | `NoTxWrap` | Disable implicit transactions when `true`.  Migrations may, and should, contain explicit `BEGIN` and `COMMIT` statements. |

## Notes

* Uses the `github.com/hanzoai/csqlite` sqlite db driver (cgo)
