package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mbeoliero/kit/log"
	"github.com/redis/go-redis/v9"
)

var ErrPushBusClosed = errors.New("push bus closed")
var ErrPushSubscriptionClosed = errors.New("push subscription closed unexpectedly")
var ErrPushEnvelopeDecode = errors.New("push envelope decode failed")
var ErrUnsupportedPushEnvelopeVersion = errors.New("unsupported push envelope version")

const currentPushEnvelopeVersion = 1

// PushBus transports cross-instance push envelopes to their owning instance.
type PushBus interface {
	PublishToInstance(ctx context.Context, instanceId string, env *PushEnvelope) error
	SubscribeInstance(
		ctx context.Context,
		instanceId string,
		handler func(context.Context, *PushEnvelope),
		onReady func(),
		onRuntimeError func(error),
	) error
	Close() error
}

// PushSubscriptionConfig controls the buffering behavior between Redis Pub/Sub
// and the local subscriber goroutine.
type PushSubscriptionConfig struct {
	ChannelSize int
	SendTimeout time.Duration
}

// RedisPushBus is the Redis Pub/Sub transport used by route-only multi-instance
// fanout.
type RedisPushBus struct {
	rdb    *redis.Client
	config PushSubscriptionConfig
	mu     sync.Mutex
	closed bool
}

// NewRedisPushBus builds a Redis-backed push bus with safe defaults for
// subscription buffering.
func NewRedisPushBus(rdb *redis.Client, config PushSubscriptionConfig) *RedisPushBus {
	if config.ChannelSize <= 0 {
		config.ChannelSize = 1024
	}
	if config.SendTimeout <= 0 {
		config.SendTimeout = 5 * time.Second
	}
	return &RedisPushBus{rdb: rdb, config: config}
}

// PublishToInstance publishes a routed envelope to the target instance channel.
func (b *RedisPushBus) PublishToInstance(ctx context.Context, instanceId string, env *PushEnvelope) error {
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return ErrPushBusClosed
	}

	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return b.rdb.Publish(ctx, pushChannelKey(instanceId), data).Err()
}

// SubscribeInstance subscribes the current instance's Redis channel and invokes
// handler synchronously for each decoded envelope. Callers should keep the
// handler lightweight and offload heavier work to local queues.
func (b *RedisPushBus) SubscribeInstance(
	ctx context.Context,
	instanceId string,
	handler func(context.Context, *PushEnvelope),
	onReady func(),
	onRuntimeError func(error),
) error {
	psb := b.rdb.Subscribe(ctx, pushChannelKey(instanceId))
	defer func() { _ = psb.Close() }()

	if _, err := psb.Receive(ctx); err != nil {
		if ctx.Err() == nil && onRuntimeError != nil {
			onRuntimeError(err)
		}
		return err
	}
	if onReady != nil {
		onReady()
	}

	ch := psb.Channel(
		redis.WithChannelSize(b.config.ChannelSize),
		redis.WithChannelSendTimeout(b.config.SendTimeout),
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				if ctx.Err() == nil && onRuntimeError != nil {
					onRuntimeError(ErrPushSubscriptionClosed)
				}
				return ErrPushSubscriptionClosed
			}

			var env PushEnvelope
			if err := json.Unmarshal([]byte(msg.Payload), &env); err != nil {
				log.CtxWarn(ctx, "push bus envelope decode failed: channel=%s err=%v", pushChannelKey(instanceId), err)
				if ctx.Err() == nil && onRuntimeError != nil {
					onRuntimeError(fmt.Errorf("%w: %v", ErrPushEnvelopeDecode, err))
				}
				continue
			}
			if env.Version != currentPushEnvelopeVersion {
				log.CtxWarn(ctx, "push bus unsupported envelope version: channel=%s push_id=%s got=%d want=%d", pushChannelKey(instanceId), env.PushId, env.Version, currentPushEnvelopeVersion)
				if ctx.Err() == nil && onRuntimeError != nil {
					onRuntimeError(fmt.Errorf("%w: got=%d want=%d push_id=%s", ErrUnsupportedPushEnvelopeVersion, env.Version, currentPushEnvelopeVersion, env.PushId))
				}
				continue
			}
			handler(ctx, &env)
		}
	}
}

// Close marks the bus as closed so future publishes fail fast.
func (b *RedisPushBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}
