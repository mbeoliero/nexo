package storetest

import (
	"testing"

	"github.com/mbeoliero/nexo/internal/store"
)

// Mem has no rollback or database row locks; these suites need neither.
func TestMem(t *testing.T) {
	s := NewMem()
	RunGroups(t, s)
	RunInsertConstraints(t, s)
	s.SetGroupMember(store.GroupMember{GroupId: "g2", UserId: "u___1"})
	for groupId, want := range map[string]int64{"g1": 2, "g2": 1, "missing": 0} {
		t.Run(groupId, func(t *testing.T) {
			if got, err := s.CountGroupMembers(t.Context(), groupId); err != nil || got != want {
				t.Fatalf("CountGroupMembers = %d, %v; want %d", got, err, want)
			}
		})
	}
}
