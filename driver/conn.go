package driver

import (
	"context"
	"database/sql/driver"
	"strings"

	"github.com/jamestjat/sqlce/engine"
)

// connector implements driver.Connector.
type connector struct {
	dsn string
}

// Connect opens the database. DSN format: "path.sdf" or "path.sdf?password=secret".
func (c *connector) Connect(_ context.Context) (driver.Conn, error) {
	path, password := parseDSN(c.dsn)
	var db *engine.Database
	var err error
	if password != "" {
		db, err = engine.OpenWithPassword(path, password)
	} else {
		db, err = engine.Open(path)
	}
	if err != nil {
		return nil, err
	}
	return &conn{db: db}, nil
}

func parseDSN(dsn string) (path, password string) {
	if idx := strings.Index(dsn, "?"); idx >= 0 {
		path = dsn[:idx]
		params := dsn[idx+1:]
		for _, part := range strings.Split(params, "&") {
			if strings.HasPrefix(part, "password=") {
				password = part[len("password="):]
			}
		}
		return
	}
	return dsn, ""
}

// Driver returns the underlying Driver.
func (c *connector) Driver() driver.Driver {
	return &Driver{}
}

var _ driver.Connector = (*connector)(nil)

// conn implements driver.Conn.
type conn struct {
	db     *engine.Database
	closed bool
}

// Prepare returns a prepared statement. SQL CE is read-only, so we parse
// the query here and execute on Query.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	return &stmt{conn: c, query: query}, nil
}

// Close closes the database connection.
func (c *conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.db.Close()
}

// Begin is not supported (read-only database).
func (c *conn) Begin() (driver.Tx, error) {
	return &tx{}, nil
}

var _ driver.Conn = (*conn)(nil)

// tx is a no-op transaction (database is read-only).
type tx struct{}

func (t *tx) Commit() error   { return nil }
func (t *tx) Rollback() error { return nil }
