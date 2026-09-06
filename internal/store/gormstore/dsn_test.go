package gormstore

import (
	"cmp"
	"strings"
	"testing"
)

// The guard has to run before the dialer, so these cases need no server.
func TestCheckMysqlDsn(t *testing.T) {
	const base = "root:nexo@tcp(127.0.0.1:3306)/nexo?parseTime=true&loc=UTC"
	for _, tc := range []struct {
		name, dsn string
		reject    bool
		want      string // substring the error must name; defaults to clientFoundRows
	}{
		{name: "clean", dsn: base},
		{name: "flag off", dsn: base + "&clientFoundRows=false"},
		{name: "no parseTime", dsn: "root:nexo@tcp(127.0.0.1:3306)/nexo?loc=UTC", reject: true, want: "parseTime"},
		{name: "flag true", dsn: base + "&clientFoundRows=true", reject: true},
		// The driver spells the flag five ways; a text match for "=true" would miss four.
		{name: "flag 1", dsn: base + "&clientFoundRows=1", reject: true},
		{name: "flag TRUE", dsn: base + "&clientFoundRows=TRUE", reject: true},
		{name: "flag True", dsn: base + "&clientFoundRows=True", reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkMysqlDsn(tc.dsn)
			if tc.reject != (err != nil) {
				t.Fatalf("checkMysqlDsn = %v, reject=%v", err, tc.reject)
			}
			if tc.reject && !strings.Contains(err.Error(), cmp.Or(tc.want, "clientFoundRows")) {
				t.Fatalf("error must name the flag: %v", err)
			}
		})
	}

	// New must refuse before dialing: an unreachable port would otherwise mask the guard.
	if _, err := New("mysql", base+"&clientFoundRows=true", 1); err == nil || !strings.Contains(err.Error(), "clientFoundRows") {
		t.Fatalf("New = %v, want the guard's error", err)
	}
}
