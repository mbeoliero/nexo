package cache

import (
	"context"
	"time"
)

const KeyPrefix = "nexo:"

// ttl <= 0 means no expiry.
type Cache interface {
	Get(ctx context.Context, key string) (val string, found bool, err error)
	Set(ctx context.Context, key, val string, ttl time.Duration) error
	SetNX(ctx context.Context, key, val string, ttl time.Duration) (bool, error)
	// Atomically delete only a matching live value; missing, expired, or mismatched keys are successful no-ops.
	DelIfValue(ctx context.Context, key, expected string) error
	Close() error
}
