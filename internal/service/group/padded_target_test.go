package group

import (
	"errors"
	"testing"

	"github.com/mbeoliero/nexo/errcode"
)

// MySQL PAD SPACE lets "u___1 " match the owner's membership row while Go's owner check compares
// the raw string; the target id is validated first so the row is never reached.
func TestKickRejectsPaddedTarget(t *testing.T) {
	s, mem, _ := setup(t)
	ctx := t.Context()
	g, err := s.Create(ctx, "u___1", CreateInput{Name: "team", MemberIds: []string{"u___2"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"u___1 ", "u___2 ", " u___2", "nx__0190 "} {
		if err := s.Kick(ctx, g.Id, "u___1", target); !errors.Is(err, errcode.ErrInvalidParam) {
			t.Errorf("kick %q: %v, want invalid param", target, err)
		}
	}
	for _, id := range []string{"u___1", "u___2"} {
		if _, err := mem.GetGroupMember(ctx, g.Id, id); err != nil {
			t.Errorf("%s must still be a member: %v", id, err)
		}
	}
}
