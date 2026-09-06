package storetest

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/store"
)

func RunGroups(t *testing.T, s store.Store) {
	ctx := t.Context()
	now := baseTime

	g := &store.Group{Id: "g1", Name: "team", OwnerId: "u___1", CreatedAt: now, UpdatedAt: now}
	members := []store.GroupMember{
		{GroupId: "g1", UserId: "u___1", Role: store.RoleOwner, JoinedAt: now},
		{GroupId: "g1", UserId: "u___2", Role: store.RoleMember, InviterUserId: "u___1", JoinedAt: now},
	}
	if err := s.CreateGroup(ctx, g, members); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.CreateGroup(ctx, g, nil); !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("duplicate group: %v", err)
	}
	if got, err := s.GetGroup(ctx, "g1"); err != nil || got.Name != "team" || got.Status != store.GroupStatusNormal {
		t.Fatalf("get: %+v %v", got, err)
	}
	if _, err := s.GetGroup(ctx, "g404"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing group: %v", err)
	}

	if err := s.AddGroupMember(ctx, &store.GroupMember{GroupId: "g1", UserId: "u___3", Role: store.RoleMember, JoinedAt: now.Add(time.Millisecond)}); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := s.AddGroupMember(ctx, &store.GroupMember{GroupId: "g1", UserId: "u___3", JoinedAt: now}); !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("duplicate member: %v", err)
	}
	if n, _ := s.CountGroupMembers(ctx, "g1"); n != 3 {
		t.Fatalf("count = %d", n)
	}
	if list, err := s.ListGroupMembers(ctx, "g1"); err != nil || len(list) != 3 || list[0].UserId != "u___1" || list[2].UserId != "u___3" {
		t.Fatalf("list: %+v %v", list, err)
	}
	if m, err := s.GetGroupMember(ctx, "g1", "u___2"); err != nil || m.Role != store.RoleMember || m.InviterUserId != "u___1" {
		t.Fatalf("member: %+v %v", m, err)
	}
	if ids, _ := s.ListUserGroupIds(ctx, "u___3"); len(ids) != 1 || ids[0] != "g1" {
		t.Fatalf("user groups: %v", ids)
	}
	if ok, err := s.RemoveGroupMember(ctx, "g1", "u___3"); err != nil || !ok {
		t.Fatalf("remove: %v %v", ok, err)
	}
	if ok, _ := s.RemoveGroupMember(ctx, "g1", "u___3"); ok {
		t.Fatal("remove twice must report false")
	}
	if _, err := s.GetGroupMember(ctx, "g1", "u___3"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("removed member: %v", err)
	}
}

func RunConversations(t *testing.T, s store.Store) {
	ctx := t.Context()
	now := baseTime
	conv := "sg_g1"

	err := s.WithTx(ctx, func(tx store.Store) error {
		c, err := tx.LockConversation(ctx, conv, store.ConversationGroup, "g1", now)
		if err != nil {
			return err
		}
		if c.MaxSeq != 0 || c.GroupId != "g1" || !c.CreatedAt.Equal(now) {
			t.Fatalf("fresh conversation: %+v", c)
		}
		return tx.UpsertUserConversation(ctx, &store.UserConversation{OwnerId: "u___1", ConversationId: conv, Type: store.ConversationGroup, GroupId: "g1", MinSeq: 1, UpdatedAt: now})
	})
	if err != nil {
		t.Fatalf("join tx: %v", err)
	}
	uc, err := s.GetUserConversation(ctx, "u___1", conv)
	if err != nil || uc.MinSeq != 1 || uc.MaxSeq != 0 || !uc.CreatedAt.Equal(now) {
		t.Fatalf("uc: %+v %v", uc, err)
	}
	if err := s.SetUserConversationMaxSeq(ctx, "u___1", conv, 7); err != nil {
		t.Fatal(err)
	}
	if uc, _ = s.GetUserConversation(ctx, "u___1", conv); uc.MaxSeq != 7 {
		t.Fatalf("quit bound: %+v", uc)
	}
	row, err := s.GetUserConversationRow(ctx, "u___1", conv)
	if err != nil || row.OwnerId != "u___1" || row.ConversationId != conv || row.MinSeq != 1 || row.MaxSeq != 7 || row.ConvMaxSeq != 0 || !row.CreatedAt.Equal(now) {
		t.Fatalf("joined permission snapshot must keep both max_seq columns: %+v %v", row, err)
	}
	visible := func(seq int64) []string {
		ids, err := s.VisibleOwners(ctx, conv, []string{"u___1", "u___9"}, seq)
		if err != nil {
			t.Fatal(err)
		}
		return ids
	}
	if got := visible(7); len(got) != 1 || got[0] != "u___1" {
		t.Fatalf("visible at upper bound: %v", got)
	}
	if got := visible(8); len(got) != 0 {
		t.Fatalf("past quit bound must be invisible: %v", got)
	}
	// re-join: min/max/read reset, created_at kept
	if err := s.UpsertUserConversation(ctx, &store.UserConversation{OwnerId: "u___1", ConversationId: conv, Type: store.ConversationGroup, GroupId: "g1", MinSeq: 8, ReadSeq: 7, UpdatedAt: now.Add(5 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if uc, _ = s.GetUserConversation(ctx, "u___1", conv); uc.MinSeq != 8 || uc.MaxSeq != 0 || uc.ReadSeq != 7 || !uc.UpdatedAt.Equal(now.Add(5*time.Millisecond)) || !uc.CreatedAt.Equal(now) {
		t.Fatalf("re-join: %+v", uc)
	}
	if got := visible(7); len(got) != 0 {
		t.Fatalf("before re-join min_seq must be invisible: %v", got)
	}
	if got := visible(8); len(got) != 1 {
		t.Fatalf("after re-join: %v", got)
	}
	if err := s.SetUserConversationOpt(ctx, "u___1", conv, new(int32(1)), new(true)); err != nil {
		t.Fatal(err)
	}
	if uc, _ = s.GetUserConversation(ctx, "u___1", conv); uc.RecvMsgOpt != 1 || !uc.IsPinned || !uc.UpdatedAt.Equal(now.Add(5*time.Millisecond)) {
		t.Fatalf("opt must not touch updated_at: %+v", uc)
	}
	// Partial update leaves the other column alone.
	if err := s.SetUserConversationOpt(ctx, "u___1", conv, nil, new(false)); err != nil {
		t.Fatal(err)
	}
	if uc, _ = s.GetUserConversation(ctx, "u___1", conv); uc.RecvMsgOpt != 1 || uc.IsPinned {
		t.Fatalf("partial opt: %+v", uc)
	}
	if err := s.SetUserConversationOpt(ctx, "u___9", conv, new(int32(0)), nil); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("opt on missing row: %v", err)
	}
	if err := s.CreateUserConversations(ctx, []store.UserConversation{
		{OwnerId: "u___7", ConversationId: conv, Type: store.ConversationGroup, GroupId: "g1", MinSeq: 1, UpdatedAt: now},
		{OwnerId: "u___8", ConversationId: conv, Type: store.ConversationGroup, GroupId: "g1", MinSeq: 1, UpdatedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	if uc, err := s.GetUserConversation(ctx, "u___8", conv); err != nil || uc.MinSeq != 1 || !uc.CreatedAt.Equal(now) {
		t.Fatalf("bulk create: %+v %v", uc, err)
	}
	if muted, err := s.MutedOwners(ctx, conv, []string{"u___1", "u___9"}); err != nil || len(muted) != 1 || muted[0] != "u___1" {
		t.Fatalf("muted: %v %v", muted, err)
	}
	if _, err := s.GetUserConversation(ctx, "u___9", conv); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing uc: %v", err)
	}
	if _, err := s.GetUserConversationRow(ctx, "u___9", conv); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing joined uc: %v", err)
	}
	if err := s.DeleteUserConversation(ctx, "u___7", conv); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetUserConversation(ctx, "u___7", conv); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
	if _, err := s.GetUserConversationRow(ctx, "u___7", conv); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("joined uc after delete: %v", err)
	}

	// Row lock serializes concurrent transactions on the same conversation.
	var wg sync.WaitGroup
	var mu sync.Mutex
	inside := 0
	overlap := false
	for range 8 {
		wg.Go(func() {
			err := s.WithTx(ctx, func(tx store.Store) error {
				if _, err := tx.LockConversation(ctx, conv, store.ConversationGroup, "g1", now); err != nil {
					return err
				}
				mu.Lock()
				inside++
				if inside > 1 {
					overlap = true
				}
				mu.Unlock()
				_ = tx.SetUserConversationMaxSeq(ctx, "u___1", conv, 0)
				mu.Lock()
				inside--
				mu.Unlock()
				return nil
			})
			if err != nil {
				t.Errorf("lock tx: %v", err)
			}
		})
	}
	wg.Wait()
	if overlap {
		t.Fatal("LockConversation did not serialize transactions")
	}
}
