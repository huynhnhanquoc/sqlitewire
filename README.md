# sqlitewire

sqlitewire is a database on SQLite files that speaks the PostgreSQL wire protocol: `psql`, pgx, JDBC and other
PostgreSQL clients connect to it over TLS, with SCRAM-SHA-256. It is written in Go only (`modernc.org/sqlite`, no cgo).

It is not a whole PostgreSQL: it speaks PostgreSQL's protocol, and rewrites the statements it supports onto SQLite.

## Packages

- `sqlite`: one writer per file; a history written in the transaction of each commit, chained by checksum, for the
  tables that keep it; anchors (a base of the file's pages, then the pages changed) and restore to a point in time;
  reading a table as it was at a past revision (`PastRelation`); backup.
- `gateway`: the sessions of the PostgreSQL port on the SQLite files: `pg_catalog` and `information_schema`, PostgreSQL
  statements rewritten for SQLite, grants checked on each statement, writes through the file's one writer, and the
  limits of a statement (rows, bytes, time, temporary files).
- `pgwire`: the PostgreSQL wire protocol, TLS, SCRAM-SHA-256 and its channel binding (SCRAM-SHA-256-PLUS), client
  certificates, the admission of statements. A server without TLS listens on loopback only: SCRAM keeps the password
  off the wire, not the statements and rows.
- `catalog`: the port's accounts, roles and grants.
- `analytics`, `columnar`: column copies of the tables a `CREATE INDEX … USING columnar` names, kept fresh by revision,
  which the port's scans read.
- `search`, `fulltext`: a table's full-text index, kept with the writer's commits (`search/README.md`).
- `sql`: the lexer the port reads statements with, PostgreSQL's type identifiers, UUIDs.
- `internal/snapshotfile`: files written whole or not at all.

sqlitewire is a Go module: the packages a server is built from. It has no command of its own yet; the Kitwork engine,
where it started as KitDB, runs it alone (`kitwork database`) and inside its own process.

## Names in a database

The names sqlitewire keeps inside a database come from its start in Kitwork and stay, so that existing files keep
opening: the tables `kitwork_history`, `kitwork_tables`, `kitwork_columns` and `kitwork_columnar`, the functions
`kitwork_pg_*`, and the accounts file `.catalog/kitwork.db`.

## License

GNU Affero General Public License, version 3: `LICENSE`.

SQLite is a registered trademark of Hipp, Wyrick & Company, Inc. sqlitewire is not affiliated with or endorsed by the
SQLite project.
