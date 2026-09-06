package conv

import "testing"

func TestSingle(t *testing.T) {
	if Single("u___2", "u___1") != "si_u___1:u___2" || Single("u___1", "u___2") != "si_u___1:u___2" {
		t.Fatal("single id must be order independent")
	}
}
