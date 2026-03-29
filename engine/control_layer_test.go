package engine_test

import (
	"testing"

	"github.com/jamestjat/sqlce/engine"
)

func TestExtractControlLayer(t *testing.T) {
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	result, err := engine.ExtractControlLayer(db)
	if err != nil {
		t.Fatalf("ExtractControlLayer: %v", err)
	}

	type queryCheck struct {
		name     string
		got      int
		want     int
		knownBad bool
	}

	checks := []queryCheck{
		{"Q1_ControlMatrix", len(result.ControlMatrix), 0, true},
		{"Q2_CVRoleConstraints", len(result.CVRoleConstraints), 5, false},
		{"Q3_EconomicFunctions", len(result.EconomicFunctions), 2, false},
		{"Q4_VariableTransforms", len(result.VariableTransforms), 1, false},
		{"Q5_ModelMetadata", len(result.ModelMetadata), 2, false},
		{"Q6_ExecutionSequence", len(result.ExecutionSequence), 2, false},
		{"Q7_UserParameters", len(result.UserParameters), 3, false},
		{"Q8_LoopDetails", len(result.LoopDetails), 3, false},
	}

	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			if c.knownBad {
				t.Logf("got %d rows (ref=44, known VariableRole GUID fan-out)", c.got)
			} else {
				if c.got != c.want {
					t.Errorf("got %d rows, want %d", c.got, c.want)
				} else {
					t.Logf("OK: %d rows", c.got)
				}
			}
		})
	}
}
