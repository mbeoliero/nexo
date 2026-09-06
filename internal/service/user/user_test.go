package user

import (
	"errors"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/cache/local"
	"github.com/mbeoliero/nexo/internal/store/storetest"
	"github.com/mbeoliero/nexo/internal/tokenstore"
)

func newService(t *testing.T) (*Service, *auth.Native) {
	t.Helper()
	c := local.New()
	t.Cleanup(func() { c.Close() })
	native := auth.NewNative("s", time.Hour, tokenstore.New(c))
	return New(storetest.NewMem(), native), native
}

func TestRegisterLoginLogout(t *testing.T) {
	ctx := t.Context()
	s, native := newService(t)

	p, err := s.Register(ctx, "alice", "secret1", "Alice")
	if err != nil || p.Id[:4] != "nx__" || p.Nickname != "Alice" {
		t.Fatalf("register: %+v %v", p, err)
	}
	if _, err := s.Register(ctx, "alice", "secret1", ""); !errors.Is(err, errcode.ErrUserExists) {
		t.Fatalf("duplicate: %v", err)
	}
	if _, err := s.Register(ctx, "bob", "short", ""); !errors.Is(err, errcode.ErrInvalidParam) {
		t.Fatalf("short password: %v", err)
	}

	if _, err := s.Login(ctx, "alice", "wrong", 5); !errors.Is(err, errcode.ErrLoginFailed) {
		t.Fatalf("wrong password: %v", err)
	}
	if _, err := s.Login(ctx, "nobody", "secret1", 5); !errors.Is(err, errcode.ErrLoginFailed) {
		t.Fatalf("unknown user must look like wrong password: %v", err)
	}
	if _, err := s.Login(ctx, "alice", "secret1", 99); !errors.Is(err, errcode.ErrInvalidParam) {
		t.Fatalf("platform_id outside 1..10 must be rejected: %v", err)
	}
	sess, err := s.Login(ctx, "alice", "secret1", 5)
	if err != nil || sess.UserId != p.Id || sess.Token == "" {
		t.Fatalf("login: %+v %v", sess, err)
	}
	id, err := native.Verify(ctx, sess.Token)
	if err != nil || id.UserId != p.Id || id.PlatformId != 5 {
		t.Fatalf("token: %+v %v", id, err)
	}
	if err := s.Logout(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := native.Verify(ctx, sess.Token); !errors.Is(err, auth.ErrTokenExpired) {
		t.Fatalf("after logout: %v", err)
	}
	if err := s.Logout(ctx, auth.Identity{Source: auth.SourceExternal}); !errors.Is(err, errcode.ErrInvalidParam) {
		t.Fatalf("external logout: %v", err)
	}
}

func TestLogoutKeepsNewerToken(t *testing.T) {
	ctx := t.Context()
	s, native := newService(t)
	if _, err := s.Register(ctx, "alice", "secret1", "Alice"); err != nil {
		t.Fatal(err)
	}
	first, err := s.Login(ctx, "alice", "secret1", 5)
	if err != nil {
		t.Fatal(err)
	}
	oldIdentity, err := native.Verify(ctx, first.Token)
	if err != nil {
		t.Fatal(err)
	}
	// The old request already passed Bearer when a concurrent login replaces its token.
	second, err := s.Login(ctx, "alice", "secret1", 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Logout(ctx, oldIdentity); err != nil {
		t.Fatal(err)
	}
	current, err := native.Verify(ctx, second.Token)
	if err != nil {
		t.Fatalf("stale logout revoked newer token: %v", err)
	}
	if _, err := native.Verify(ctx, first.Token); !errors.Is(err, auth.ErrTokenExpired) {
		t.Fatalf("old token must stay revoked: %v", err)
	}
	missingToken := current
	missingToken.TokenId = ""
	if err := s.Logout(ctx, missingToken); !errors.Is(err, errcode.ErrInvalidParam) {
		t.Fatalf("logout without token id must be rejected: %v", err)
	}
	if _, err := native.Verify(ctx, second.Token); err != nil {
		t.Fatalf("invalid embedded logout changed current token: %v", err)
	}
	if err := s.Logout(ctx, current); err != nil {
		t.Fatal(err)
	}
	if _, err := native.Verify(ctx, second.Token); !errors.Is(err, auth.ErrTokenExpired) {
		t.Fatalf("current logout must revoke its own token: %v", err)
	}
	if err := s.Logout(ctx, current); err != nil {
		t.Fatalf("repeated embedded logout must be a no-op: %v", err)
	}
}

func TestGetUpdate(t *testing.T) {
	ctx := t.Context()
	s, _ := newService(t)
	p, _ := s.Register(ctx, "alice", "secret1", "Alice")

	if _, err := s.Get(ctx, "u___404"); !errors.Is(err, errcode.ErrUserNotFound) {
		t.Fatalf("missing: %v", err)
	}
	avatar := "http://a"
	got, err := s.Update(ctx, p.Id, Update{Avatar: &avatar})
	if err != nil || got.Avatar != avatar || got.Nickname != "Alice" {
		t.Fatalf("partial update must keep other fields: %+v %v", got, err)
	}
	if _, err := s.Update(ctx, "u___404", Update{}); !errors.Is(err, errcode.ErrUserNotFound) {
		t.Fatalf("update missing: %v", err)
	}
	many, err := s.GetMany(ctx, []string{p.Id, "u___404"})
	if err != nil || len(many) != 1 || many[0] != got {
		t.Fatalf("GetMany: %v %v", many, err)
	}
	if empty, err := s.GetMany(ctx, nil); err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("GetMany empty: %v %v, want non-nil empty slice", empty, err)
	}
}

// server.Server.User() hands this service to embedding hosts whether or not the native provider is
// enabled, so the password paths must refuse rather than dereference a nil *auth.Native.
func TestNativeDisabledIsAnErrorNotAPanic(t *testing.T) {
	ctx := t.Context()
	s := New(storetest.NewMem(), nil)

	if _, err := s.Register(ctx, "alice", "secret1", "Alice"); !errors.Is(err, errcode.ErrProviderDisabled) {
		t.Fatalf("register: %v", err)
	}
	if _, err := s.Login(ctx, "alice", "secret1", 5); !errors.Is(err, errcode.ErrProviderDisabled) {
		t.Fatalf("login: %v", err)
	}
	if err := s.Logout(ctx, auth.Identity{UserId: "nx__1", TokenId: "t1"}); !errors.Is(err, errcode.ErrProviderDisabled) {
		t.Fatalf("logout: %v", err)
	}
}
