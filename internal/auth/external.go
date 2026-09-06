package auth

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"

	"github.com/mbeoliero/nexo/internal/identity"
)

// Platform-issued HS256 tokens. Revocation belongs to the platform, so no TokenStore lookup.
type External struct {
	secrets     [][]byte
	defaultRole string
}

type externalClaims struct {
	UserId int64  `json:"user_id"`
	Role   string `json:"role,omitempty"`
	jwt.RegisteredClaims
}

func NewExternal(secrets []string, defaultRole string) *External {
	e := &External{defaultRole: defaultRole}
	for _, s := range secrets {
		e.secrets = append(e.secrets, []byte(s))
	}
	return e
}

func (e *External) Verify(_ context.Context, token string) (Identity, error) {
	var claims externalClaims
	var lastErr error
	for _, secret := range e.secrets {
		claims = externalClaims{}
		_, err := jwt.ParseWithClaims(token, &claims, func(*jwt.Token) (any, error) { return secret, nil }, hs256Only)
		if err == nil {
			lastErr = nil
			break
		}
		lastErr = err
		// The signature verified and only the claims failed: no other secret changes that, and
		// continuing would overwrite an expired error with the next secret's signature error.
		if !errors.Is(err, jwt.ErrTokenSignatureInvalid) {
			break
		}
	}
	if lastErr != nil {
		return Identity{}, mapJwtErr(lastErr)
	}
	if claims.ExpiresAt == nil || claims.UserId <= 0 {
		return Identity{}, fmt.Errorf("%w: exp or user_id missing", ErrTokenInvalid)
	}
	role := cmp.Or(identity.Role(claims.Role), identity.Role(e.defaultRole))
	userId, err := identity.Actor{Id: claims.UserId, Role: role}.UserId()
	if err != nil {
		return Identity{}, fmt.Errorf("%w: %w", ErrTokenInvalid, err)
	}
	sum := sha256.Sum256([]byte(token))
	return Identity{
		UserId:    userId,
		Role:      string(role),
		TokenId:   hex.EncodeToString(sum[:16]),
		ExpiresAt: claims.ExpiresAt.UnixMilli(),
		Source:    SourceExternal,
	}, nil
}

var hs256Only = jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()})

func mapJwtErr(err error) error {
	if errors.Is(err, jwt.ErrTokenExpired) {
		return ErrTokenExpired
	}
	return fmt.Errorf("%w: %w", ErrTokenInvalid, err)
}
