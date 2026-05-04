// Package driver implements database/sql/driver interfaces for SQL CE files.
//
// Usage:
//
//	import (
//	    "database/sql"
//	    _ "github.com/jamestjat/sqlce/driver"
//	)
//
//	db, err := sql.Open("sqlce", "path/to/database.sdf")
//	rows, err := db.Query("SELECT * FROM TableName")
package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("sqlce", &Driver{})
}

// Driver implements database/sql/driver.Driver.
type Driver struct{}

// Open returns a new connection to the database.
// The name is the file path to the .sdf file, optionally with ?password=secret.
// If the DSN has no password parameter, SQLCE_PASSWORD is used when set.
func (d *Driver) Open(name string) (driver.Conn, error) {
	c := &connector{dsn: name}
	return c.Connect(context.TODO())
}

var _ driver.Driver = (*Driver)(nil)
