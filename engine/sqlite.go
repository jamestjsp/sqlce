package engine

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/jamestjat/sqlce/format"

	_ "modernc.org/sqlite"
)

// ExportResult holds the SQLite DB and any non-fatal warnings from export.
type ExportResult struct {
	DB       *sql.DB
	Warnings []error
}

// ExportToSQLite loads all tables from the SDF database into an in-memory
// SQLite database and returns it. Non-fatal per-table errors are collected
// in ExportResult.Warnings. The caller must close the returned DB.
func ExportToSQLite(db *Database) (*ExportResult, error) {
	sqliteDB, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		return nil, fmt.Errorf("creating in-memory SQLite: %w", err)
	}

	res := &ExportResult{DB: sqliteDB}
	warn := func(table string, msg string, err error) {
		res.Warnings = append(res.Warnings, fmt.Errorf("export %s: %s: %w", table, msg, err))
	}

	for _, name := range db.Tables() {
		tbl, err := db.Table(name)
		if err != nil {
			warn(name, "open", err)
			continue
		}
		result, err := tbl.Scan()
		if err != nil {
			warn(name, "scan", err)
			continue
		}
		cols := tbl.Columns()
		if len(cols) == 0 {
			continue
		}

		createSQL := BuildCreateTable(name, cols)
		if _, err := sqliteDB.Exec(createSQL); err != nil {
			warn(name, "create table", err)
			continue
		}
		if len(result.Rows) == 0 {
			continue
		}

		placeholders := make([]string, len(cols))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		insertSQL := fmt.Sprintf(`INSERT INTO "%s" VALUES (%s)`, name, strings.Join(placeholders, ","))

		tx, err := sqliteDB.Begin()
		if err != nil {
			warn(name, "begin", err)
			continue
		}
		stmt, err := tx.Prepare(insertSQL)
		if err != nil {
			tx.Rollback()
			warn(name, "prepare", err)
			continue
		}
		var execErr error
		for _, row := range result.Rows {
			args := make([]any, len(cols))
			for i := range cols {
				if i < len(row) {
					args[i] = row[i]
				}
			}
			if _, err := stmt.Exec(args...); err != nil {
				execErr = err
				break
			}
		}
		stmt.Close()
		if execErr != nil {
			tx.Rollback()
			warn(name, "insert", execErr)
			continue
		}
		if err := tx.Commit(); err != nil {
			warn(name, "commit", err)
		}
	}

	return res, nil
}

func BuildCreateTable(name string, cols []format.ColumnDef) string {
	var parts []string
	for _, col := range cols {
		sqlType := ceTypeToSQLite(col.TypeID)
		parts = append(parts, fmt.Sprintf(`"%s" %s`, col.Name, sqlType))
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s)`, name, strings.Join(parts, ", "))
}

func ceTypeToSQLite(typeID uint16) string {
	switch typeID {
	case format.TypeTinyInt, format.TypeSmallInt, format.TypeInt, format.TypeBigInt, format.TypeBit:
		return "INTEGER"
	case format.TypeFloat, format.TypeReal:
		return "REAL"
	case format.TypeMoney, format.TypeNumeric:
		return "NUMERIC"
	case format.TypeNVarchar, format.TypeNChar, format.TypeNText, format.TypeUniqueIdentifier:
		return "TEXT"
	case format.TypeDatetime:
		return "TEXT"
	case format.TypeImage, format.TypeBinary, format.TypeVarBinary, format.TypeRowVersion:
		return "BLOB"
	default:
		return "TEXT"
	}
}

