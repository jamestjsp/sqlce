package engine

import (
	"strings"
	"testing"

	"github.com/jamestjat/sqlce/format"
)

func TestBuildCreateTableEscapesIdentifiers(t *testing.T) {
	sql := BuildCreateTable(`bad"table`, []format.ColumnDef{
		{Name: `bad"column`, TypeID: format.TypeInt},
		{Name: `name`, TypeID: format.TypeNVarchar},
	})

	if !strings.Contains(sql, `"bad""table"`) {
		t.Fatalf("table identifier was not escaped: %s", sql)
	}
	if !strings.Contains(sql, `"bad""column" INTEGER`) {
		t.Fatalf("column identifier was not escaped: %s", sql)
	}
}
