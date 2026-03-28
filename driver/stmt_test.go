package driver

import (
	"testing"
)

func TestQueryParse_SelectAll(t *testing.T) {
	pq, err := parseSelectQuery("SELECT * FROM Properties")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if pq.Table != "Properties" {
		t.Errorf("table: expected 'Properties', got %q", pq.Table)
	}
	if len(pq.Columns) != 0 {
		t.Errorf("columns: expected empty (all), got %v", pq.Columns)
	}
}

func TestQueryParse_SelectColumns(t *testing.T) {
	pq, err := parseSelectQuery("SELECT Name, Value FROM Properties")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if pq.Table != "Properties" {
		t.Errorf("table: %q", pq.Table)
	}
	if len(pq.Columns) != 2 || pq.Columns[0] != "Name" || pq.Columns[1] != "Value" {
		t.Errorf("columns: %v", pq.Columns)
	}
}

func TestQueryParse_QuotedTableDouble(t *testing.T) {
	pq, err := parseSelectQuery(`SELECT * FROM "My Table"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if pq.Table != "My Table" {
		t.Errorf("table: %q", pq.Table)
	}
}

func TestQueryParse_QuotedTableBracket(t *testing.T) {
	pq, err := parseSelectQuery("SELECT * FROM [My Table]")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if pq.Table != "My Table" {
		t.Errorf("table: %q", pq.Table)
	}
}

func TestQueryParse_WithObjectID(t *testing.T) {
	pq, err := parseSelectQuery("SELECT * FROM Properties WITH OBJECTID 1305")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if pq.Table != "Properties" {
		t.Errorf("table: %q", pq.Table)
	}
	if pq.ObjectID != 1305 {
		t.Errorf("objectID: expected 1305, got %d", pq.ObjectID)
	}
}

func TestQueryParse_CaseInsensitive(t *testing.T) {
	pq, err := parseSelectQuery("select * from Properties")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if pq.Table != "Properties" {
		t.Errorf("table: %q", pq.Table)
	}
}

func TestQueryParse_QuotedColumns(t *testing.T) {
	pq, err := parseSelectQuery(`SELECT "Name", [Value] FROM Properties`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(pq.Columns) != 2 || pq.Columns[0] != "Name" || pq.Columns[1] != "Value" {
		t.Errorf("columns: %v", pq.Columns)
	}
}

func TestQueryParse_RejectInsert(t *testing.T) {
	_, err := parseSelectQuery("INSERT INTO Properties VALUES ('a', 'b')")
	if err == nil {
		t.Error("expected error for INSERT")
	}
}

func TestQueryParse_RejectWhere(t *testing.T) {
	_, err := parseSelectQuery("SELECT * FROM Properties WHERE Name = 'x'")
	if err == nil {
		t.Error("expected error for WHERE")
	}
}

func TestQueryParse_RejectJoin(t *testing.T) {
	_, err := parseSelectQuery("SELECT * FROM A JOIN B ON A.id = B.id")
	if err == nil {
		t.Error("expected error for JOIN")
	}
}

func TestQueryParse_MissingFrom(t *testing.T) {
	_, err := parseSelectQuery("SELECT *")
	if err == nil {
		t.Error("expected error for missing FROM")
	}
}

func TestQueryParse_MissingTable(t *testing.T) {
	_, err := parseSelectQuery("SELECT * FROM ")
	if err == nil {
		t.Error("expected error for missing table")
	}
}
