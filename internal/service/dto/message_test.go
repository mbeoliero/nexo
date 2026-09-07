package dto

import "testing"

// One product rule, one definition: the HTTP handler and the WS dispatcher both resolve
// sender_read through this, so a change here cannot leave one transport on the old behaviour.
func TestSendRequestSenderReadFor(t *testing.T) {
	yes, no := true, false
	for _, tc := range []struct {
		name     string
		ptr      *bool
		internal bool
		want     bool
	}{
		{"client default", nil, false, true},
		{"platform default", nil, true, false},
		{"client opts out", &no, false, false},
		{"platform opts in", &yes, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := (SendRequest{SenderRead: tc.ptr}).SenderReadFor(tc.internal); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
