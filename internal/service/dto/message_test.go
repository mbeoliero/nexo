package dto

import (
	"testing"

	"github.com/mbeoliero/nexo/internal/auth"
)

// One product rule, one definition: the HTTP handler and the WS dispatcher both resolve
// sender_read through this, so a change here cannot leave one transport on the old behaviour.
func TestSendRequestSenderReadFor(t *testing.T) {
	yes, no := true, false
	for _, tc := range []struct {
		name   string
		ptr    *bool
		source string
		want   bool
	}{
		{"native default", nil, auth.SourceNative, true},
		{"external default", nil, auth.SourceExternal, true},
		{"platform default", nil, auth.SourceInternal, false},
		{"client opts out", &no, auth.SourceNative, false},
		{"platform opts in", &yes, auth.SourceInternal, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := (SendRequest{SenderRead: tc.ptr}).SenderReadFor(tc.source); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
