package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/mbeoliero/nexo/internal/cache"
	"github.com/mbeoliero/nexo/internal/cache/local"
	"github.com/mbeoliero/nexo/internal/tokenstore"
)

func signExternal(t *testing.T, secret string, claims jwt.MapClaims) string {
	t.Helper()
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestExternal(t *testing.T) {
	ctx := t.Context()
	exp := time.Now().Add(time.Hour).Unix()
	e := NewExternal([]string{"old", "new"}, "user")

	id, err := e.Verify(ctx, signExternal(t, "new", jwt.MapClaims{"user_id": 42, "exp": exp}))
	if err != nil || id.UserId != "u___42" || id.Role != "user" || id.PlatformId != 0 || id.Source != SourceExternal || id.TokenId == "" {
		t.Fatalf("user token: %+v %v", id, err)
	}
	id, err = e.Verify(ctx, signExternal(t, "old", jwt.MapClaims{"user_id": 7, "role": "agent", "exp": exp}))
	if err != nil || id.UserId != "ag__7" {
		t.Fatalf("rotated secret + agent: %+v %v", id, err)
	}

	bad := map[string]string{
		"wrong secret": signExternal(t, "other", jwt.MapClaims{"user_id": 1, "exp": exp}),
		"no exp":       signExternal(t, "new", jwt.MapClaims{"user_id": 1}),
		"bad role":     signExternal(t, "new", jwt.MapClaims{"user_id": 1, "role": "bot", "exp": exp}),
		"garbage":      "not.a.jwt",
	}
	for name, tok := range bad {
		if _, err := e.Verify(ctx, tok); !errors.Is(err, ErrTokenInvalid) {
			t.Errorf("%s: got %v, want ErrTokenInvalid", name, err)
		}
	}
	if _, err := e.Verify(ctx, signExternal(t, "new", jwt.MapClaims{"user_id": 1, "exp": time.Now().Add(-time.Hour).Unix()})); !errors.Is(err, ErrTokenExpired) {
		t.Errorf("expired: got %v", err)
	}
	// AllSecrets is newest-first, so during rotation the signing secret is not the last one tried:
	// the loop must not overwrite the expiry with the next secret's signature error (clients would
	// hard-log-out instead of refreshing).
	rotating := NewExternal([]string{"new", "old"}, "user")
	if _, err := rotating.Verify(ctx, signExternal(t, "new", jwt.MapClaims{"user_id": 1, "exp": time.Now().Add(-time.Hour).Unix()})); !errors.Is(err, ErrTokenExpired) {
		t.Errorf("expired under rotation: got %v, want ErrTokenExpired", err)
	}
	none := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{"user_id": 1, "exp": exp})
	tok, _ := none.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if _, err := e.Verify(ctx, tok); !errors.Is(err, ErrTokenInvalid) {
		t.Errorf("alg=none must be rejected, got %v", err)
	}
}

func TestNative(t *testing.T) {
	ctx := t.Context()
	c := local.New()
	defer c.Close()
	n := NewNative("s", time.Hour, tokenstore.New(c))

	t1, _, err := n.Issue(ctx, "nx__1", 5)
	if err != nil {
		t.Fatal(err)
	}
	id, err := n.Verify(ctx, t1)
	if err != nil || id.UserId != "nx__1" || id.PlatformId != 5 || id.Source != SourceNative {
		t.Fatalf("verify: %+v %v", id, err)
	}
	if _, err := NewExternal([]string{"s"}, "user").Verify(ctx, t1); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("native token must not pass external provider: %v", err)
	}

	t2, _, err := n.Issue(ctx, "nx__1", 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Verify(ctx, t1); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("old token after re-login: got %v, want ErrTokenExpired", err)
	}
	id, err = n.Verify(ctx, t2)
	if err != nil {
		t.Fatalf("new token: %v", err)
	}
	if err := n.Revoke(ctx, "nx__1", 5, id.TokenId); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Verify(ctx, t2); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("after logout: got %v", err)
	}

	n.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	t3, _, _ := NewNative("s", time.Hour, tokenstore.New(c)).Issue(ctx, "nx__1", 5)
	if _, err := n.Verify(ctx, t3); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("clock past exp: got %v", err)
	}
}

func TestChain(t *testing.T) {
	ctx := t.Context()
	c := local.New()
	defer c.Close()
	n := NewNative("native-secret", time.Hour, tokenstore.New(c))
	e := NewExternal([]string{"ext-secret"}, "user")
	chain := Chain{e, n}

	nt, _, _ := n.Issue(ctx, "nx__1", 5)
	if id, err := chain.Verify(ctx, nt); err != nil || id.Source != SourceNative {
		t.Fatalf("chain native: %+v %v", id, err)
	}
	et := signExternal(t, "ext-secret", jwt.MapClaims{"user_id": 9, "exp": time.Now().Add(time.Hour).Unix()})
	if id, err := chain.Verify(ctx, et); err != nil || id.UserId != "u___9" {
		t.Fatalf("chain external: %+v %v", id, err)
	}
	if _, err := chain.Verify(ctx, "junk"); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("chain junk: %v", err)
	}
	if _, err := (Chain{}).Verify(ctx, et); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("empty chain: %v", err)
	}
	// A native token that is expired / hit a TokenStore outage keeps that meaning even when a
	// later provider answers ErrTokenInvalid.
	down := NewNative("native-secret", time.Hour, tokenstore.New(downCache{c}))
	if _, err := (Chain{down, e}).Verify(ctx, nt); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("native unavailable then external: %v, want ErrUnavailable", err)
	}
}

// downCache fails every read, like a TokenStore whose backend is unreachable.
type downCache struct{ cache.Cache }

func (downCache) Get(context.Context, string) (string, bool, error) {
	return "", false, errors.New("cache down")
}
