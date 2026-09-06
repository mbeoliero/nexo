package bus

import (
	"context"
	"time"
)

const (
	MinBackoff = time.Second
	MaxBackoff = 30 * time.Second
)

// Sleep waits for the current backoff and returns the next one (doubling, capped).
func Sleep(ctx context.Context, d time.Duration) (time.Duration, error) {
	select {
	case <-ctx.Done():
		return d, ctx.Err()
	case <-time.After(d):
		return min(d*2, MaxBackoff), nil
	}
}
