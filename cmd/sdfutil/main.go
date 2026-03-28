// sdfutil provides command-line access to SQL CE database files.
//
// Usage:
//
//	sdfutil info <file.sdf>          Show database header info
//	sdfutil tables <file.sdf>        List all tables
//	sdfutil schema <file.sdf> <tbl>  Show table schema
//	sdfutil dump <file.sdf> <tbl> <objectID>  Dump table rows (tab-separated)
package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/jamestjat/sqlce/engine"
	"github.com/jamestjat/sqlce/format"

	_ "modernc.org/sqlite"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "info":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: sdfutil info <file.sdf>")
			os.Exit(1)
		}
		cmdInfo(os.Args[2])
	case "tables":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: sdfutil tables <file.sdf>")
			os.Exit(1)
		}
		cmdTables(os.Args[2])
	case "schema":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "usage: sdfutil schema <file.sdf> <table>")
			os.Exit(1)
		}
		cmdSchema(os.Args[2], os.Args[3])
	case "dump":
		if len(os.Args) < 5 {
			fmt.Fprintln(os.Stderr, "usage: sdfutil dump <file.sdf> <table> <objectID>")
			os.Exit(1)
		}
		objID, err := strconv.Atoi(os.Args[4])
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid objectID: %s\n", os.Args[4])
			os.Exit(1)
		}
		cmdDump(os.Args[2], os.Args[3], uint16(objID))
	case "export":
		outputFormat := "csv"
		for i, arg := range os.Args {
			if arg == "--format" && i+1 < len(os.Args) {
				outputFormat = os.Args[i+1]
			}
		}
		if outputFormat == "sqlite" {
			if len(os.Args) < 4 {
				fmt.Fprintln(os.Stderr, "usage: sdfutil export --format sqlite <file.sdf> <output.db>")
				os.Exit(1)
			}
			sdfPath := ""
			outputPath := ""
			for _, arg := range os.Args[2:] {
				if arg == "--format" || arg == "sqlite" {
					continue
				}
				if sdfPath == "" {
					sdfPath = arg
				} else if outputPath == "" {
					outputPath = arg
				}
			}
			if sdfPath == "" || outputPath == "" {
				fmt.Fprintln(os.Stderr, "usage: sdfutil export --format sqlite <file.sdf> <output.db>")
				os.Exit(1)
			}
			cmdExportSQLite(sdfPath, outputPath)
		} else {
			if len(os.Args) < 5 {
				fmt.Fprintln(os.Stderr, "usage: sdfutil export <file.sdf> <table> <objectID> [--format csv|json]")
				os.Exit(1)
			}
			objID, err := strconv.Atoi(os.Args[4])
			if err != nil {
				fmt.Fprintf(os.Stderr, "invalid objectID: %s\n", os.Args[4])
				os.Exit(1)
			}
			cmdExport(os.Args[2], os.Args[3], uint16(objID), outputFormat)
		}
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `sdfutil — SQL CE database file utility

Commands:
  info   <file.sdf>                                 Show database header info
  tables <file.sdf>                                 List all tables
  schema <file.sdf> <table>                         Show table column schema
  dump   <file.sdf> <table> <objectID>              Dump rows (tab-separated)
  export <file.sdf> <table> <objectID>              Export (--format csv|json)
  export --format sqlite <file.sdf> <output.db>     Export all tables to SQLite`)
}

func cmdInfo(path string) {
	db, err := engine.Open(path)
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	h := db.Header()
	enc := "no"
	if h.Encrypted {
		enc = fmt.Sprintf("yes (type %d)", h.EncryptionType)
	}

	fmt.Printf("File:       %s\n", path)
	fmt.Printf("Version:    %s\n", h.Version)
	fmt.Printf("Page size:  %d bytes\n", h.PageSize)
	fmt.Printf("Pages:      %d\n", db.TotalPages())
	fmt.Printf("File size:  %d bytes\n", db.TotalPages()*h.PageSize)
	fmt.Printf("LCID:       %d\n", h.LCID)
	fmt.Printf("Encrypted:  %s\n", enc)
	fmt.Printf("Tables:     %d\n", db.TableCount())
}

func cmdTables(path string) {
	db, err := engine.Open(path)
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	tables := db.Tables()
	for _, name := range tables {
		tbl, _ := db.Table(name)
		fmt.Printf("%-40s %d columns\n", name, tbl.ColumnCount())
	}
	fmt.Printf("\nTotal: %d tables\n", len(tables))
}

func cmdSchema(path, tableName string) {
	db, err := engine.Open(path)
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	tbl, err := db.Table(tableName)
	if err != nil {
		fatal(err)
	}

	schema := tbl.Schema()
	fmt.Printf("Table: %s (%d columns)\n\n", schema.Name(), schema.ColumnCount())
	fmt.Printf("%-4s %-30s %-20s %-8s %s\n", "#", "Name", "Type", "Length", "Variable")
	fmt.Printf("%-4s %-30s %-20s %-8s %s\n", "---", "---", "---", "---", "---")

	for _, col := range schema.Columns() {
		varStr := ""
		if col.IsVariable() {
			varStr = "yes"
		}
		fmt.Printf("%-4d %-30s %-20s %-8d %s\n",
			col.Ordinal(), col.Name(), col.Type(), col.MaxLength(), varStr)
	}
}

func cmdDump(path, tableName string, objectID uint16) {
	db, err := engine.Open(path)
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	tbl, err := db.Table(tableName)
	if err != nil {
		fatal(err)
	}

	ri, err := tbl.RowsWithObjectID(objectID)
	if err != nil {
		fatal(err)
	}
	defer ri.Close()

	// Print header
	cols := ri.Columns()
	fmt.Println(strings.Join(cols, "\t"))

	// Print rows
	count := 0
	for ri.Next() {
		vals := ri.Values()
		parts := make([]string, len(vals))
		for i, v := range vals {
			if v == nil {
				parts[i] = "NULL"
			} else {
				parts[i] = fmt.Sprintf("%v", v)
			}
		}
		fmt.Println(strings.Join(parts, "\t"))
		count++
	}
	fmt.Fprintf(os.Stderr, "\n%d rows\n", count)
}

func cmdExport(path, tableName string, objectID uint16, outputFormat string) {
	db, err := engine.Open(path)
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	tbl, err := db.Table(tableName)
	if err != nil {
		fatal(err)
	}

	ri, err := tbl.RowsWithObjectID(objectID)
	if err != nil {
		fatal(err)
	}
	defer ri.Close()

	switch outputFormat {
	case "csv":
		exportCSV(ri)
	case "json":
		exportJSON(ri)
	default:
		fmt.Fprintf(os.Stderr, "unsupported format: %s (use csv or json)\n", outputFormat)
		os.Exit(1)
	}
}

func exportCSV(ri *engine.RowIterator) {
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	w.Write(ri.Columns())

	for ri.Next() {
		vals := ri.Values()
		record := make([]string, len(vals))
		for i, v := range vals {
			if v == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", v)
			}
		}
		w.Write(record)
	}
}

func exportJSON(ri *engine.RowIterator) {
	cols := ri.Columns()
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	var allRows []map[string]any
	for ri.Next() {
		vals := ri.Values()
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			if i < len(vals) {
				row[c] = vals[i]
			}
		}
		allRows = append(allRows, row)
	}
	enc.Encode(allRows)
}

func cmdExportSQLite(sdfPath, outputPath string) {
	db, err := engine.Open(sdfPath)
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	os.Remove(outputPath)
	sqliteDB, err := sql.Open("sqlite", outputPath)
	if err != nil {
		fatal(fmt.Errorf("creating SQLite: %w", err))
	}
	defer sqliteDB.Close()

	exported := 0
	skipped := 0
	totalRows := 0

	for _, name := range db.Tables() {
		tbl, err := db.Table(name)
		if err != nil {
			skipped++
			continue
		}

		result, err := tbl.Scan()
		if err != nil {
			skipped++
			fmt.Fprintf(os.Stderr, "  skip %s: %v\n", name, err)
			continue
		}

		cols := tbl.Columns()
		if len(cols) == 0 {
			skipped++
			continue
		}

		createSQL := buildCreateTable(name, cols)
		if _, err := sqliteDB.Exec(createSQL); err != nil {
			fmt.Fprintf(os.Stderr, "  skip %s: create table: %v\n", name, err)
			skipped++
			continue
		}

		if len(result.Rows) == 0 {
			exported++
			continue
		}

		placeholders := make([]string, len(cols))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		insertSQL := fmt.Sprintf(`INSERT INTO "%s" VALUES (%s)`, name, strings.Join(placeholders, ","))

		tx, err := sqliteDB.Begin()
		if err != nil {
			skipped++
			continue
		}

		insertStmt, err := tx.Prepare(insertSQL)
		if err != nil {
			tx.Rollback()
			fmt.Fprintf(os.Stderr, "  skip %s: prepare: %v\n", name, err)
			skipped++
			continue
		}

		for _, row := range result.Rows {
			args := make([]any, len(cols))
			for i := range cols {
				if i < len(row) {
					args[i] = row[i]
				}
			}
			insertStmt.Exec(args...)
		}

		insertStmt.Close()
		tx.Commit()

		totalRows += len(result.Rows)
		exported++
		fmt.Fprintf(os.Stderr, "  %s: %d rows\n", name, len(result.Rows))
	}

	fmt.Fprintf(os.Stderr, "\nExported %d tables (%d rows), skipped %d\n", exported, totalRows, skipped)
}

func buildCreateTable(name string, cols []format.ColumnDef) string {
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

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
