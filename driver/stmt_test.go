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

func TestQueryParse_RejectTrailingText(t *testing.T) {
	tests := []string{
		"SELECT * FROM Properties extra",
		"SELECT * FROM Properties;",
		"SELECT * FROM Properties WITH",
		"SELECT * FROM Properties WITH OBJECTID",
		"SELECT * FROM Properties WITH OBJECTID 1305 extra",
		"SELECT * FROM Properties WITH OBJECTID 1305;",
		"SELECT * FROM Properties WITH OBJECTID -1",
		"SELECT * FROM Properties WITH OBJECTID 65536",
		"SELECT * FROM Properties WITH OBJECTID abc",
	}
	for _, query := range tests {
		t.Run(query, func(t *testing.T) {
			if _, err := parseSelectQuery(query); err == nil {
				t.Fatal("expected parse error")
			}
		})
	}
}

func TestQueryParse_ObjectIDBounds(t *testing.T) {
	tests := []struct {
		query string
		want  uint16
	}{
		{"SELECT * FROM Properties WITH OBJECTID 0", 0},
		{"SELECT * FROM Properties WITH OBJECTID 65535", 65535},
		{`SELECT * FROM "Properties" WITH OBJECTID 1305`, 1305},
		{"SELECT * FROM [Properties] WITH OBJECTID 1305", 1305},
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			pq, err := parseSelectQuery(tc.query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if pq.ObjectID != tc.want {
				t.Fatalf("ObjectID = %d, want %d", pq.ObjectID, tc.want)
			}
		})
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
