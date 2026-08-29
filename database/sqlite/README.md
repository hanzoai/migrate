# sqlite

`sqlite://path/to/database?query`

Unlike other migrate database drivers, the sqlite driver will automatically wrap each migration in an implicit transaction by default.  Migrations must not contain explicit `BEGIN` or `COMMIT` statements.  This behavior may change in a future major release.  (See below for a workaround.)

The auxiliary query parameters listed below may be supplied to tailor migrate behavior.  All auxiliary query parameters are optional.

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table.  Defaults to `schema_migrations`. |
| `x-no-tx-wrap` | `NoTxWrap` | Disable implicit transactions when `true`.  Migrations may, and should, contain explicit `BEGIN` and `COMMIT` statements. |

## Notes

* Uses the `github.com/hanzoai/sqlite` driver, which registers the `sqlite` name over the pure-Go engine when cgo is off and over `github.com/hanzoai/csqlite` when it is on
  * The pure-Go engine has [limited `GOOS` and `GOARCH` support](https://pkg.go.dev/modernc.org/sqlite?utm_source=godoc#hdr-Supported_platforms_and_architectures)
