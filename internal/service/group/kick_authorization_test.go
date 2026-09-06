package group

import (
	"context"
	"errors"
	"testing"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/service/conv"
	"github.com/mbeoliero/nexo/internal/store"
)

type beforeRemoveStore struct {
	store.Store
	beforeTx func()
}

func (s beforeRemoveStore) WithTx(ctx context.Context, fn func(store.Store) error) error {
	s.beforeTx()
	return s.Store.WithTx(ctx, fn)
}

func TestKickUsesCurrentMembership(t *testing.T) {
	for _, tc := range []struct {
		name string
		want error
	}{
		{name: "operator removed", want: errcode.ErrNotGroupMember},
		{name: "operator demoted", want: errcode.ErrNotGroupAdmin},
		{name: "target promoted", want: errcode.ErrNoPermission},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner, mem, _ := setup(t)
			ctx := t.Context()
			g, err := owner.Create(ctx, "u___1", CreateInput{Name: "team", MemberIds: []string{"u___2", "u___3"}})
			if err != nil {
				t.Fatal(err)
			}
			admin, err := mem.GetGroupMember(ctx, g.Id, "u___2")
			if err != nil {
				t.Fatal(err)
			}
			admin.Role = store.RoleAdmin
			mem.SetGroupMember(*admin)

			// Change membership after the request's preflight but before its transaction starts.
			delayed := New(Adapt(beforeRemoveStore{Store: mem, beforeTx: func() {
				switch tc.name {
				case "operator removed":
					if err := owner.Kick(ctx, g.Id, "u___1", "u___2"); err != nil {
						t.Fatal(err)
					}
				case "operator demoted":
					admin.Role = store.RoleMember
					mem.SetGroupMember(*admin)
				case "target promoted":
					target, err := mem.GetGroupMember(ctx, g.Id, "u___3")
					if err != nil {
						t.Fatal(err)
					}
					target.Role = store.RoleAdmin
					mem.SetGroupMember(*target)
				}
			}}), NoopNotifier{}, 3)
			if err := delayed.Kick(ctx, g.Id, "u___2", "u___3"); !errors.Is(err, tc.want) {
				t.Errorf("kick after %s: got %v, want %v", tc.name, err, tc.want)
			}
			if _, err := mem.GetGroupMember(ctx, g.Id, "u___3"); err != nil {
				t.Errorf("target membership must remain: %v", err)
			}
			if _, err := mem.GetUserConversation(ctx, "u___3", conv.Group(g.Id)); err != nil {
				t.Errorf("target conversation must remain: %v", err)
			}
		})
	}
}
