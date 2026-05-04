# Security

`sqlce` is a read-only parser and exporter for SQL Server Compact Edition `.sdf`
files. Treat every `.sdf` file as untrusted input unless it comes from a source
you control.

## Security Model

- The engine and `database/sql` driver are read-only. They do not modify `.sdf`
  files and do not execute SQL stored inside the database.
- The SQL driver supports a small `SELECT` subset for choosing tables and
  columns. It is not a general SQL Server or SQLite execution engine.
- Table names, column names, and row values can be attacker-controlled when the
  input file is untrusted. Consumers should validate names before using them in
  generated SQL, file names, logs, HTML, shell commands, or other interpreters.
- CSV, JSON, and SQLite exports preserve source data. Opening exported files in
  spreadsheet, database, or BI tools can trigger behavior controlled by the
  exported data, such as spreadsheet formulas or downstream SQL interpretation.
- Passwords for encrypted databases are provided by the caller or CLI operator.
  Avoid placing passwords in command history, logs, crash reports, or shared
  configuration files.

## Processing Hostile Files

Run hostile or unknown `.sdf` files with the same precautions used for other
binary parsers:

- Process files in a sandboxed account, container, or temporary workspace with
  the least filesystem access needed.
- Apply resource limits appropriate for your environment, especially for large
  files or batch conversion jobs.
- Write exports to explicit output paths. SQLite export refuses to overwrite an
  existing file unless `--force` is supplied.
- Treat exported CSV, JSON, and SQLite files as untrusted artifacts until they
  have been reviewed or sanitized for their target environment.

## Reporting Vulnerabilities

Please report suspected security vulnerabilities privately to the repository
maintainers before publishing details. Include:

- A minimal reproducer or sample file if you can share one.
- The affected command or API.
- The expected and observed behavior.
- Any crash, panic, data exposure, overwrite, or resource-exhaustion impact.

Do not open a public issue with an exploit or sensitive sample database unless a
maintainer asks you to do so.
