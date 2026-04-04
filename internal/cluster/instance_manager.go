package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mbeoliero/nexo/pkg/constant"
)

type syncRequest struct {
	ctx  context.Context
	done chan error
}

var ErrInstanceStateSyncNotConfigured = errors.New("instance state sync not configured")

type InstanceManager struct {
	rdb               *redis.Client
	stateTTL          time.Duration
	heartbeatInterval time.Duration
	now               func() time.Time
	writeStateFn      func(context.Context, InstanceState) error
	mu                sync.Mutex
	running           bool
	syncCh            chan syncRequest
	instanceId        string
	snapshot          func() InstanceState
}

var _ InstanceStateReader = (*InstanceManager)(nil)
var _ InstanceStatePublisher = (*InstanceManager)(nil)

func NewInstanceManager(rdb *redis.Client, stateTTL time.Duration) *InstanceManager {
	return &InstanceManager{
		rdb:      rdb,
		stateTTL: stateTTL,
		now:      time.Now,
	}
}

func (m *InstanceManager) SetHeartbeatInterval(interval time.Duration) {
	m.heartbeatInterval = interval
}

func (m *InstanceManager) HeartbeatInterval() time.Duration {
	return m.effectiveHeartbeatInterval()
}

func (m *InstanceManager) ConfigureSnapshot(instanceId string, snapshot func() InstanceState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.instanceId = instanceId
	m.snapshot = snapshot
}

func (m *InstanceManager) WriteState(ctx context.Context, state InstanceState) error {
	if state.UpdatedAt == 0 {
		state.UpdatedAt = m.now().UnixMilli()
	}

	if m.writeStateFn != nil {
		return m.writeStateFn(ctx, state)
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return m.rdb.Set(ctx, instanceAliveKey(state.InstanceId), data, m.stateTTL).Err()
}

func (m *InstanceManager) ReadState(ctx context.Context, instanceId string) (*InstanceState, error) {
	data, err := m.rdb.Get(ctx, instanceAliveKey(instanceId)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	var state InstanceState
	if err = json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (m *InstanceManager) SyncNow(ctx context.Context) error {
	m.mu.Lock()
	running := m.running
	syncCh := m.syncCh
	instanceId := m.instanceId
	snapshot := m.snapshot
	m.mu.Unlock()

	if snapshot == nil || instanceId == "" {
		return ErrInstanceStateSyncNotConfigured
	}
	if !running || syncCh == nil {
		return m.writeSnapshot(ctx, instanceId, snapshot)
	}

	req := syncRequest{
		ctx:  ctx,
		done: make(chan error, 1),
	}

	select {
	case syncCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *InstanceManager) Run(ctx context.Context, instanceId string, snapshot func() InstanceState) error {
	m.mu.Lock()
	if m.syncCh == nil {
		m.syncCh = make(chan syncRequest)
	}
	m.instanceId = instanceId
	m.snapshot = snapshot
	m.running = true
	syncCh := m.syncCh
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.running = false
		m.mu.Unlock()
	}()

	write := func() error {
		state := snapshot()
		state.InstanceId = instanceId
		state.UpdatedAt = m.now().UnixMilli()
		return m.WriteState(ctx, state)
	}

	if err := write(); err != nil {
		return err
	}

	ticker := time.NewTicker(m.effectiveHeartbeatInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req := <-syncCh:
			req.done <- m.writeSnapshot(req.ctx, instanceId, snapshot)
		case <-ticker.C:
			if err := write(); err != nil {
				return err
			}
		}
	}
}

func (m *InstanceManager) writeSnapshot(ctx context.Context, instanceId string, snapshot func() InstanceState) error {
	state := snapshot()
	state.InstanceId = instanceId
	state.UpdatedAt = m.now().UnixMilli()
	return m.WriteState(ctx, state)
}

func (m *InstanceManager) effectiveHeartbeatInterval() time.Duration {
	if m.heartbeatInterval > 0 {
		return m.heartbeatInterval
	}
	if m.stateTTL <= 0 {
		return time.Second
	}
	interval := m.stateTTL / 3
	if interval < 10*time.Millisecond {
		return 10 * time.Millisecond
	}
	return interval
}

func instanceAliveKey(instanceId string) string {
	return constant.GetRedisKeyPrefix() + fmt.Sprintf("cluster:instance:alive:%s", instanceId)
}
