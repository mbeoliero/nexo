// Shared suite; each implementation's _test.go runs it against a migrated DB.
package storetest

import (
	"errors"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/store"
)

// baseTime is the fixture clock every suite writes with, so rows across suites compare cleanly.
var baseTime = time.UnixMilli(1_700_000_000_000).UTC()

func RunUsers(t *testing.T, s store.Store) {
	ctx := t.Context()
	now := baseTime

	platform := &store.User{Id: "u___1", Nickname: "a", CreatedAt: now, UpdatedAt: now}
	if err := s.UpsertUser(ctx, platform); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	platform.Nickname, platform.UpdatedAt = "b", now.Add(1*time.Millisecond)
	if err := s.UpsertUser(ctx, platform); err != nil {
		t.Fatalf("upsert again: %v", err)
	}
	got, err := s.GetUser(ctx, "u___1")
	if err != nil || got.Nickname != "b" || !got.CreatedAt.Equal(now) || !got.UpdatedAt.Equal(now.Add(1*time.Millisecond)) {
		t.Fatalf("after upsert: %+v, %v", got, err)
	}

	// Partial profile update: only the given column moves.
	if err := s.UpdateUserProfile(ctx, "u___1", nil, new("http://av"), nil, now.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if got, _ = s.GetUser(ctx, "u___1"); got.Nickname != "b" || got.Avatar != "http://av" || !got.UpdatedAt.Equal(now.Add(2*time.Millisecond)) {
		t.Fatalf("partial update: %+v", got)
	}
	if err := s.UpdateUserProfile(ctx, "u___1", nil, new("http://av"), nil, now.Add(2*time.Millisecond)); err != nil {
		t.Fatalf("same values in the same millisecond must succeed: %v", err)
	}
	if err := s.UpdateUserProfile(ctx, "u___404", new("x"), nil, nil, now); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("update missing: %v", err)
	}

	native := &store.User{Id: "nx__1", Username: "alice", PasswordHash: "h", CreatedAt: now, UpdatedAt: now}
	if err := s.CreateUser(ctx, native); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.CreateUser(ctx, &store.User{Id: "nx__1", CreatedAt: now, UpdatedAt: now}); !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("duplicate id: got %v, want ErrDuplicate", err)
	}
	if got, err := s.GetUserByUsername(ctx, "alice"); err != nil || got.Id != "nx__1" || got.PasswordHash != "h" {
		t.Fatalf("by username: %+v, %v", got, err)
	}

	if _, err := s.GetUser(ctx, "u___404"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing: got %v, want ErrNotFound", err)
	}
	if users, err := s.GetUsers(ctx, []string{"u___1", "nx__1", "u___404"}); err != nil || len(users) != 2 {
		t.Fatalf("GetUsers: %d users, %v", len(users), err)
	}
	if users, err := s.GetUsers(ctx, nil); err != nil || len(users) != 0 {
		t.Fatalf("GetUsers(nil): %d users, %v", len(users), err)
	}

	rollback := errors.New("rollback")
	err = s.WithTx(ctx, func(tx store.Store) error {
		if err := tx.UpsertUser(ctx, &store.User{Id: "u___tx", CreatedAt: now, UpdatedAt: now}); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("WithTx should return fn error, got %v", err)
	}
	if _, err := s.GetUser(ctx, "u___tx"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("tx should have rolled back, got %v", err)
	}

	// The Store a callback receives is borrowed: it must refuse to nest, and Close on it must
	// not take the process-wide pool down with every other in-flight request.
	err = s.WithTx(ctx, func(tx store.Store) error {
		tx.Close()
		return tx.WithTx(ctx, func(store.Store) error { return nil })
	})
	if !errors.Is(err, store.ErrNestedTx) {
		t.Fatalf("nested WithTx: got %v, want ErrNestedTx", err)
	}
	if _, err := s.GetUser(ctx, "u___1"); err != nil {
		t.Fatalf("store unusable after a callback called Close on its borrowed Store: %v", err)
	}
}
