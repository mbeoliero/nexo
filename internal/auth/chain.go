package auth

import (
	"context"
	"errors"
)

// First provider that accepts the token wins. When none does, an error with a specific meaning
// (ErrUnavailable, ErrTokenExpired) from an earlier provider is kept over a later provider's plain
// ErrTokenInvalid: a native token that hit a TokenStore outage must not be reported as invalid.
type Chain []Authenticator

func (c Chain) Verify(ctx context.Context, token string) (Identity, error) {
	var lastErr error = ErrTokenInvalid
	for _, a := range c {
		id, err := a.Verify(ctx, token)
		if err == nil {
			return id, nil
		}
		if !terminal(lastErr) || terminal(err) {
			lastErr = err
		}
	}
	return Identity{}, lastErr
}

func terminal(err error) bool {
	return errors.Is(err, ErrUnavailable) || errors.Is(err, ErrTokenExpired)
}
