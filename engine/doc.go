// Package engine provides high-level access to SQL CE database files.
//
// The main entry point is [Open], which returns a [Database] handle.
// From there, use [Database.Tables] to list tables and [Database.Table]
// to get a [Table] handle for reading schema and data.
//
// # Example
//
//	db, err := engine.Open("database.sdf")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	for _, name := range db.Tables() {
//	    fmt.Println(name)
//	}
package engine
