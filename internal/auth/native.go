package auth

import (
	"context"
	"fmt"
	"time"
	"uuid"

	"github.com/golang-jwt/jwt/v5"

	"github.com/mbeoliero/nexo/internal/identity"
	"github.com/mbeoliero/nexo/internal/tokenstore"
)

// Self-issued HS256 tokens for native accounts; one live token per (user, platform) via TokenStore.
type Native struct {
	secret []byte
	ttl    time.Duration
	tokens *tokenstore.TokenStore
	now    func() time.Time
}

type nativeClaims struct {
	PlatformId int `json:"pid"`
	jwt.RegisteredClaims
}

func NewNative(secret string, ttl time.Duration, tokens *tokenstore.TokenStore) *Native {
	return &Native{secret: []byte(secret), ttl: ttl, tokens: tokens, now: time.Now}
}

// Issue signs a new token and makes it the only valid one for (userId, platformId).
func (n *Native) Issue(ctx context.Context, userId string, platformId int) (token string, expiresAt time.Time, err error) {
	now := n.now()
	expiresAt = now.Add(n.ttl)
	claims := nativeClaims{
		PlatformId: platformId,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userId,
			ID:        uuid.NewV7().String(),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
		},
	}
	token, err = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(n.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("auth: sign: %w", err)
	}
	if err := n.tokens.Set(ctx, userId, platformId, claims.ID, n.ttl); err != nil {
		return "", time.Time{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return token, expiresAt, nil
}

func (n *Native) Revoke(ctx context.Context, userId string, platformId int, tokenId string) error {
	if err := n.tokens.Delete(ctx, userId, platformId, tokenId); err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return nil
}

func (n *Native) Verify(ctx context.Context, token string) (Identity, error) {
	var claims nativeClaims
	_, err := jwt.ParseWithClaims(token, &claims, func(*jwt.Token) (any, error) { return n.secret, nil },
		hs256Only, jwt.WithTimeFunc(n.now))
	if err != nil {
		return Identity{}, mapJwtErr(err)
	}
	if claims.ExpiresAt == nil || claims.ID == "" || !identity.IsNative(claims.Subject) || claims.PlatformId < 1 || claims.PlatformId > MaxPlatformId {
		return Identity{}, fmt.Errorf("%w: malformed native claims", ErrTokenInvalid)
	}
	return n.check(ctx, claims)
}

// Check re-validates a live connection's token against TokenStore without re-parsing.
func (n *Native) Check(ctx context.Context, id Identity) error {
	ok, err := n.tokens.Check(ctx, id.UserId, id.PlatformId, id.TokenId)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if !ok {
		return fmt.Errorf("%w: superseded", ErrTokenExpired)
	}
	return nil
}

func (n *Native) check(ctx context.Context, claims nativeClaims) (Identity, error) {
	id := Identity{
		UserId:     claims.Subject,
		Role:       string(identity.RoleUser),
		PlatformId: claims.PlatformId,
		TokenId:    claims.ID,
		ExpiresAt:  claims.ExpiresAt.UnixMilli(),
		Source:     SourceNative,
	}
	if err := n.Check(ctx, id); err != nil {
		return Identity{}, err
	}
	return id, nil
}
