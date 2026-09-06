package tokenstore

import (
	"context"
	"fmt"
	"time"

	"github.com/mbeoliero/nexo/internal/cache"
)

// Native tokens only: one live token per (user, platform). Set overwrites,
// which is how a new login invalidates the previous one on the same platform.
type TokenStore struct {
	c cache.Cache
}

func New(c cache.Cache) *TokenStore { return &TokenStore{c: c} }

func key(userId string, platformId int) string {
	return fmt.Sprintf("%stok:%s:%d", cache.KeyPrefix, userId, platformId)
}

func (s *TokenStore) Set(ctx context.Context, userId string, platformId int, tokenId string, ttl time.Duration) error {
	return s.c.Set(ctx, key(userId, platformId), tokenId, ttl)
}

func (s *TokenStore) Check(ctx context.Context, userId string, platformId int, tokenId string) (bool, error) {
	cur, found, err := s.c.Get(ctx, key(userId, platformId))
	if err != nil {
		return false, err
	}
	return found && cur == tokenId, nil
}

func (s *TokenStore) Delete(ctx context.Context, userId string, platformId int, tokenId string) error {
	return s.c.DelIfValue(ctx, key(userId, platformId), tokenId)
}
