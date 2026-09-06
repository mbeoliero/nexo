package auth

import (
	"context"
	"errors"
)

// MaxPlatformId bounds platform ids (docs/integration.md: 1..10); native tokens and login enforce it.
const MaxPlatformId = 10

var (
	ErrTokenInvalid = errors.New("auth: token invalid")
	ErrTokenExpired = errors.New("auth: token expired")
	// ErrUnavailable is returned when a backing store (TokenStore) fails; callers fail closed.
	ErrUnavailable = errors.New("auth: unavailable")
)

const (
	SourceExternal = "external"
	SourceNative   = "native"
)

type Identity struct {
	UserId     string
	Role       string
	PlatformId int // 0 for external tokens: the request supplies it
	TokenId    string
	ExpiresAt  int64 // unix ms
	Source     string
}

type Authenticator interface {
	Verify(ctx context.Context, token string) (Identity, error)
}
