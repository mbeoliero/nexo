package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/entity"
	"github.com/mbeoliero/nexo/pkg/constant"
)

type stubLocalDispatcher struct {
	mu          sync.Mutex
	localByUser map[string][]ConnRef
	deliveries  [][]ConnRef
	payloads    []*PushPayload
}

func (s *stubLocalDispatcher) DeliverLocal(_ context.Context, refs []ConnRef, payload *PushPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := append([]ConnRef(nil), refs...)
	s.deliveries = append(s.deliveries, copied)
	s.payloads = append(s.payloads, payload)
	return nil
}

func (s *stubLocalDispatcher) deliveryCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.deliveries)
}

func (s *stubLocalDispatcher) LocalConnRefs(userIDs []string, excludeConnID string) map[string][]ConnRef {
	result := make(map[string][]ConnRef, len(userIDs))
	for _, userID := range userIDs {
		for _, ref := range s.localByUser[userID] {
			if excludeConnID != "" && ref.ConnId == excludeConnID {
				continue
			}
			result[userID] = append(result[userID], ref)
		}
	}
	return result
}

type stubRouteReader struct {
	refs map[string][]RouteConn
	err  error
}

func (s *stubRouteReader) GetUsersConnRefs(_ context.Context, userIDs []string) (map[string][]RouteConn, error) {
	if s.err != nil {
		return nil, s.err
	}
	result := make(map[string][]RouteConn, len(userIDs))
	for _, userID := range userIDs {
		result[userID] = append(result[userID], s.refs[userID]...)
	}
	return result, nil
}

type blockingRouteReader struct {
	started chan struct{}
}

func (s *blockingRouteReader) GetUsersConnRefs(ctx context.Context, _ []string) (map[string][]RouteConn, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

type pausingRouteReader struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *pausingRouteReader) GetUsersConnRefs(context.Context, []string) (map[string][]RouteConn, error) {
	s.once.Do(func() {
		select {
		case s.started <- struct{}{}:
		default:
		}
		<-s.release
	})
	return map[string][]RouteConn{}, nil
}

type blockingLocalDispatcher struct {
	started chan struct{}
	release chan struct{}
}

func (s *blockingLocalDispatcher) DeliverLocal(_ context.Context, _ []ConnRef, _ *PushPayload) error {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-s.release
	return nil
}

func (s *blockingLocalDispatcher) LocalConnRefs([]string, string) map[string][]ConnRef {
	return map[string][]ConnRef{}
}

type publishedEnvelope struct {
	instanceID string
	env        *PushEnvelope
}

type stubPushBus struct {
	mu        sync.Mutex
	published []publishedEnvelope
}

func (s *stubPushBus) PublishToInstance(_ context.Context, instanceID string, env *PushEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.published = append(s.published, publishedEnvelope{instanceID: instanceID, env: env})
	return nil
}

func (s *stubPushBus) SubscribeInstance(_ context.Context, _ string, _ func(context.Context, *PushEnvelope), _ func(), _ func(error)) error {
	return nil
}

func (s *stubPushBus) Close() error { return nil }

type flakyPushBus struct {
	mu          sync.Mutex
	publishErrs []error
	publishCall int
	published   []publishedEnvelope
}

func (s *flakyPushBus) PublishToInstance(_ context.Context, instanceID string, env *PushEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishCall++
	if len(s.publishErrs) > 0 {
		err := s.publishErrs[0]
		s.publishErrs = s.publishErrs[1:]
		if err != nil {
			return err
		}
	}
	s.published = append(s.published, publishedEnvelope{instanceID: instanceID, env: env})
	return nil
}

func (s *flakyPushBus) SubscribeInstance(_ context.Context, _ string, _ func(context.Context, *PushEnvelope), _ func(), _ func(error)) error {
	return nil
}

func (s *flakyPushBus) Close() error { return nil }

type routeReadFailureRecorder struct {
	errs chan error
}

func (r *routeReadFailureRecorder) handle(err error) {
	if r == nil || r.errs == nil {
		return
	}
	r.errs <- err
}

func TestProcessPushTaskPushesLocalBeforePublishingRemote(t *testing.T) {
	local := &stubLocalDispatcher{
		localByUser: map[string][]ConnRef{
			"u1": {{UserId: "u1", ConnId: "c1", PlatformId: 5}},
		},
	}
	bus := &stubPushBus{}
	coordinator := NewPushCoordinator("i1", local, &stubRouteReader{
		refs: map[string][]RouteConn{
			"u1": {{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}, {UserId: "u1", ConnId: "c2", InstanceId: "i2", PlatformId: 1}},
		},
	}, bus)

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")
	waitDispatchDrained(t, coordinator)

	if len(local.deliveries) != 1 {
		t.Fatalf("local delivery count mismatch: %d", len(local.deliveries))
	}
	if len(bus.published) != 1 || bus.published[0].instanceID != "i2" {
		t.Fatalf("published envelopes mismatch: %+v", bus.published)
	}
}

func TestDispatchRouteOnlyPublishesPerInstanceEnvelope(t *testing.T) {
	local := &stubLocalDispatcher{}
	bus := &stubPushBus{}
	coordinator := NewPushCoordinator("i1", local, &stubRouteReader{
		refs: map[string][]RouteConn{
			"u1": {
				{UserId: "u1", ConnId: "c1", InstanceId: "i2", PlatformId: 5},
				{UserId: "u1", ConnId: "c2", InstanceId: "i2", PlatformId: 1},
				{UserId: "u1", ConnId: "c3", InstanceId: "i3", PlatformId: 2},
			},
		},
	}, bus)

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")
	waitDispatchDrained(t, coordinator)

	if len(bus.published) != 2 {
		t.Fatalf("published envelope count mismatch: %d", len(bus.published))
	}
	for _, published := range bus.published {
		if published.env.PushId == "" {
			t.Fatalf("expected push id to be populated: %+v", published.env)
		}
		if published.env.SentAt == 0 {
			t.Fatalf("expected sent at to be populated: %+v", published.env)
		}
	}
}

func TestDispatchRouteOnlyChunksLargePerInstanceEnvelope(t *testing.T) {
	local := &stubLocalDispatcher{}
	bus := &stubPushBus{}
	coordinator := NewPushCoordinator("i1", local, &stubRouteReader{
		refs: map[string][]RouteConn{
			"u1": {
				{UserId: "u1", ConnId: "c1", InstanceId: "i2", PlatformId: 5},
				{UserId: "u1", ConnId: "c2", InstanceId: "i2", PlatformId: 1},
				{UserId: "u1", ConnId: "c3", InstanceId: "i2", PlatformId: 2},
				{UserId: "u1", ConnId: "c4", InstanceId: "i3", PlatformId: 3},
			},
		},
	}, bus)
	coordinator.ConfigureDispatch(8, 1, 2)

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")
	waitDispatchDrained(t, coordinator)

	if len(bus.published) != 3 {
		t.Fatalf("published envelope count mismatch: %d", len(bus.published))
	}

	var i2ChunkCount int
	for _, published := range bus.published {
		if published.instanceID == "i2" {
			i2ChunkCount++
			if got := len(published.env.TargetConnMap["i2"]); got > 2 {
				t.Fatalf("chunk size mismatch: got %d want <= 2", got)
			}
		}
	}
	if i2ChunkCount != 2 {
		t.Fatalf("expected two chunks for i2, got %d", i2ChunkCount)
	}
}

func TestLocalOnlyModeKeepsSingleInstanceRealtimePush(t *testing.T) {
	local := &stubLocalDispatcher{
		localByUser: map[string][]ConnRef{
			"u1": {{UserId: "u1", ConnId: "c1", PlatformId: 5}},
		},
	}
	coordinator := NewPushCoordinator("i1", local, nil, nil)

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")
	waitDispatchDrained(t, coordinator)

	if len(local.deliveries) != 1 {
		t.Fatalf("local delivery count mismatch: %d", len(local.deliveries))
	}
}

func TestRouteReadFailureStillDeliversLocalButInvokesFailureHandler(t *testing.T) {
	local := &stubLocalDispatcher{
		localByUser: map[string][]ConnRef{
			"u1": {{UserId: "u1", ConnId: "c1", PlatformId: 5}},
		},
	}
	bus := &stubPushBus{}
	coordinator := NewPushCoordinator("i1", local, &stubRouteReader{err: errors.New("boom")}, bus)
	stats := NewRuntimeStats()
	coordinator.SetRuntimeStats(stats)
	failures := &routeReadFailureRecorder{errs: make(chan error, 1)}
	coordinator.SetRouteReadFailureHandler(failures.handle)

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")
	waitDispatchDrained(t, coordinator)

	if len(local.deliveries) != 1 {
		t.Fatalf("local delivery count mismatch: %d", len(local.deliveries))
	}
	if len(bus.published) != 0 {
		t.Fatalf("expected no remote publish on route read failure: %+v", bus.published)
	}
	if got := stats.Snapshot().RouteReadErrorsTotal; got != 1 {
		t.Fatalf("route read error count mismatch: got %d want 1", got)
	}
	select {
	case err := <-failures.errs:
		if err == nil || err.Error() != "boom" {
			t.Fatalf("unexpected route read failure callback error: %v", err)
		}
	default:
		t.Fatal("expected route read failure handler to be invoked")
	}
}

func TestCanceledRouteReadDoesNotCountAsFailure(t *testing.T) {
	routeReader := &blockingRouteReader{started: make(chan struct{}, 1)}
	stats := NewRuntimeStats()
	coordinator := NewPushCoordinator("i1", &stubLocalDispatcher{}, routeReader, &stubPushBus{})
	coordinator.SetRuntimeStats(stats)
	failures := &routeReadFailureRecorder{errs: make(chan error, 1)}
	coordinator.SetRouteReadFailureHandler(failures.handle)

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")

	select {
	case <-routeReader.started:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not start route lookup")
	}

	coordinator.CancelDispatches()
	waitDispatchDrained(t, coordinator)

	if got := stats.Snapshot().RouteReadErrorsTotal; got != 0 {
		t.Fatalf("route read error count mismatch on cancellation: got %d want 0", got)
	}
	select {
	case err := <-failures.errs:
		t.Fatalf("did not expect route read failure callback on cancellation, got %v", err)
	default:
	}
}

func TestRemoteEnvelopeDeliversWithoutRedisLookup(t *testing.T) {
	local := &stubLocalDispatcher{}
	coordinator := NewPushCoordinator("i1", local, nil, nil)

	coordinator.HandleRemoteEnvelope(context.Background(), &PushEnvelope{
		Version:        1,
		SourceInstance: "i2",
		TargetConnMap: map[string][]ConnRef{
			"i1": {{UserId: "u1", ConnId: "c1", PlatformId: 5}},
		},
		Payload: &PushPayload{Message: &PushMessage{ConversationId: "si_u1_u2"}},
	})

	if len(local.deliveries) != 1 || len(local.deliveries[0]) != 1 || local.deliveries[0][0].ConnId != "c1" {
		t.Fatalf("remote delivery mismatch: %+v", local.deliveries)
	}
}

func TestExcludeConnIDIsAppliedBeforeLocalAndRemoteGrouping(t *testing.T) {
	local := &stubLocalDispatcher{
		localByUser: map[string][]ConnRef{
			"u1": {{UserId: "u1", ConnId: "c1", PlatformId: 5}, {UserId: "u1", ConnId: "skip", PlatformId: 1}},
		},
	}
	bus := &stubPushBus{}
	coordinator := NewPushCoordinator("i1", local, &stubRouteReader{
		refs: map[string][]RouteConn{
			"u1": {
				{UserId: "u1", ConnId: "skip", InstanceId: "i2", PlatformId: 1},
				{UserId: "u1", ConnId: "c2", InstanceId: "i2", PlatformId: 5},
			},
		},
	}, bus)

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "skip")
	waitDispatchDrained(t, coordinator)

	if len(local.deliveries[0]) != 1 || local.deliveries[0][0].ConnId != "c1" {
		t.Fatalf("local refs mismatch after exclude: %+v", local.deliveries)
	}
	if len(bus.published) != 1 || len(bus.published[0].env.TargetConnMap["i2"]) != 1 || bus.published[0].env.TargetConnMap["i2"][0].ConnId != "c2" {
		t.Fatalf("remote refs mismatch after exclude: %+v", bus.published)
	}
}

func TestNeverPublishesToSourceInstanceEvenIfRouteStoreReturnsLocalRefs(t *testing.T) {
	local := &stubLocalDispatcher{}
	bus := &stubPushBus{}
	coordinator := NewPushCoordinator("i1", local, &stubRouteReader{
		refs: map[string][]RouteConn{
			"u1": {{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}},
		},
	}, bus)

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")
	waitDispatchDrained(t, coordinator)

	if len(bus.published) != 0 {
		t.Fatalf("expected no publish to source instance: %+v", bus.published)
	}
}

func TestAcceptedDispatchIsTrackedUntilLocalAndRemoteAttemptsFinish(t *testing.T) {
	local := &stubLocalDispatcher{
		localByUser: map[string][]ConnRef{
			"u1": {{UserId: "u1", ConnId: "c1", PlatformId: 5}},
		},
	}
	bus := &stubPushBus{}
	coordinator := NewPushCoordinator("i1", local, &stubRouteReader{
		refs: map[string][]RouteConn{
			"u1": {{UserId: "u1", ConnId: "c2", InstanceId: "i2", PlatformId: 5}},
		},
	}, bus)

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.WaitDispatchDrained(ctx); err != nil {
		t.Fatalf("wait dispatch drained: %v", err)
	}
	if coordinator.PendingDispatch() != 0 {
		t.Fatalf("pending dispatch mismatch: %d", coordinator.PendingDispatch())
	}
}

func TestAsyncPushToUsersDoesNotLeakPendingWhenWorkerFinishesBeforeEnqueueReturns(t *testing.T) {
	local := &stubLocalDispatcher{
		localByUser: map[string][]ConnRef{
			"u1": {{UserId: "u1", ConnId: "c1", PlatformId: 5}},
		},
	}
	coordinator := NewPushCoordinator("i1", local, nil, nil)
	coordinator.ConfigureDispatch(1, 1, 500)

	blockAddPending := make(chan struct{})
	coordinator.afterTaskQueued = func() {
		<-blockAddPending
	}

	done := make(chan struct{})
	go func() {
		coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")
		close(done)
	}()

	waitForCondition(t, time.Second, func() bool {
		return local.deliveryCount() == 1
	})
	close(blockAddPending)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("async push did not return")
	}

	waitDispatchDrained(t, coordinator)
	if got := coordinator.PendingDispatch(); got != 0 {
		t.Fatalf("pending dispatch mismatch: got %d want 0", got)
	}
}

func TestAsyncPushToUsersDropsWhenDispatchQueueFull(t *testing.T) {
	routeReader := &blockingRouteReader{started: make(chan struct{}, 1)}
	stats := NewRuntimeStats()
	coordinator := NewPushCoordinator("i1", &stubLocalDispatcher{}, routeReader, &stubPushBus{})
	coordinator.ConfigureDispatch(1, 1, 500)
	coordinator.SetRuntimeStats(stats)

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")

	select {
	case <-routeReader.started:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not start route lookup")
	}

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")
	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")

	waitForCondition(t, time.Second, func() bool {
		return stats.Snapshot().DispatchDroppedTotal == 1
	})

	if got := coordinator.PendingDispatch(); got != 2 {
		t.Fatalf("pending dispatch mismatch: got %d want 2", got)
	}
	if got := stats.Snapshot().DispatchQueueDepth; got > 1 {
		t.Fatalf("dispatch queue depth mismatch: got %d want <= 1", got)
	}

	coordinator.CancelDispatches()
	waitDispatchDrained(t, coordinator)
}

func TestEnqueueRemoteEnvelopeParticipatesInDispatchDrain(t *testing.T) {
	local := &blockingLocalDispatcher{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	coordinator := NewPushCoordinator("i1", local, nil, nil)
	coordinator.ConfigureDispatch(1, 1, 500)

	coordinator.EnqueueRemoteEnvelope(context.Background(), &PushEnvelope{
		Version: 1,
		TargetConnMap: map[string][]ConnRef{
			"i1": {{UserId: "u1", ConnId: "c1", PlatformId: 5}},
		},
		Payload: &PushPayload{Message: &PushMessage{ConversationId: "si_u1_u2"}},
	})

	select {
	case <-local.started:
	case <-time.After(time.Second):
		t.Fatal("remote envelope did not start local delivery")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := coordinator.WaitDispatchDrained(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait dispatch drained should stay blocked on remote backlog, got %v", err)
	}

	close(local.release)
	waitDispatchDrained(t, coordinator)
}

func TestEnqueueRemoteEnvelopeTriggersOverflowHandlerInsteadOfBlocking(t *testing.T) {
	local := &blockingLocalDispatcher{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	coordinator := NewPushCoordinator("i1", local, nil, nil)
	coordinator.ConfigureDispatch(1, 1, 500)

	overflow := make(chan struct{}, 1)
	coordinator.SetRemoteOverflowHandler(func() {
		overflow <- struct{}{}
	})

	first := &PushEnvelope{
		Version: 1,
		TargetConnMap: map[string][]ConnRef{
			"i1": {{UserId: "u1", ConnId: "c1", PlatformId: 5}},
		},
		Payload: &PushPayload{Message: &PushMessage{ConversationId: "si_u1_u2"}},
	}
	second := &PushEnvelope{
		Version: 1,
		TargetConnMap: map[string][]ConnRef{
			"i1": {{UserId: "u2", ConnId: "c2", PlatformId: 1}},
		},
		Payload: &PushPayload{Message: &PushMessage{ConversationId: "si_u1_u2"}},
	}
	third := &PushEnvelope{
		Version: 1,
		TargetConnMap: map[string][]ConnRef{
			"i1": {{UserId: "u3", ConnId: "c3", PlatformId: 2}},
		},
		Payload: &PushPayload{Message: &PushMessage{ConversationId: "si_u1_u2"}},
	}

	coordinator.EnqueueRemoteEnvelope(context.Background(), first)

	select {
	case <-local.started:
	case <-time.After(time.Second):
		t.Fatal("first remote envelope did not start local delivery")
	}

	coordinator.EnqueueRemoteEnvelope(context.Background(), second)

	done := make(chan struct{})
	go func() {
		coordinator.EnqueueRemoteEnvelope(context.Background(), third)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("third remote envelope blocked on overflow")
	}

	select {
	case <-overflow:
	case <-time.After(time.Second):
		t.Fatal("expected overflow handler to be invoked")
	}

	close(local.release)
}

func TestCancelDispatchesUnblocksBlockedAsyncDispatch(t *testing.T) {
	routeReader := &blockingRouteReader{started: make(chan struct{}, 1)}
	coordinator := NewPushCoordinator("i1", &stubLocalDispatcher{}, routeReader, &stubPushBus{})

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")

	select {
	case <-routeReader.started:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not start route lookup")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := coordinator.WaitDispatchDrained(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected wait to time out before cancellation, got %v", err)
	}

	coordinator.CancelDispatches()
	waitDispatchDrained(t, coordinator)
}

func TestRemotePublishRetriesBeforeReportingFailure(t *testing.T) {
	bus := &flakyPushBus{
		publishErrs: []error{
			errors.New("temporary publish failure"),
			errors.New("temporary publish failure"),
			nil,
		},
	}
	coordinator := NewPushCoordinator("i1", &stubLocalDispatcher{}, &stubRouteReader{
		refs: map[string][]RouteConn{
			"u1": {{UserId: "u1", ConnId: "c1", InstanceId: "i2", PlatformId: 5}},
		},
	}, bus)
	coordinator.publishRetryLimit = 3

	failures := make(chan error, 1)
	coordinator.SetRemotePublishFailureHandler(func(err error) {
		failures <- err
	})

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")
	waitDispatchDrained(t, coordinator)

	if got := bus.publishCall; got != 3 {
		t.Fatalf("publish retry count mismatch: got %d want 3", got)
	}
	if len(bus.published) != 1 {
		t.Fatalf("expected publish to eventually succeed: %+v", bus.published)
	}
	select {
	case err := <-failures:
		t.Fatalf("did not expect publish failure callback after retry recovery, got %v", err)
	default:
	}
}

func TestRemotePublishFailureInvokesHandlerAfterRetriesExhausted(t *testing.T) {
	bus := &flakyPushBus{
		publishErrs: []error{
			errors.New("temporary publish failure"),
			errors.New("temporary publish failure"),
			errors.New("temporary publish failure"),
		},
	}
	coordinator := NewPushCoordinator("i1", &stubLocalDispatcher{}, &stubRouteReader{
		refs: map[string][]RouteConn{
			"u1": {{UserId: "u1", ConnId: "c1", InstanceId: "i2", PlatformId: 5}},
		},
	}, bus)
	coordinator.publishRetryLimit = 3
	stats := NewRuntimeStats()
	coordinator.SetRuntimeStats(stats)

	failures := make(chan error, 1)
	coordinator.SetRemotePublishFailureHandler(func(err error) {
		failures <- err
	})

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")
	waitDispatchDrained(t, coordinator)

	if got := bus.publishCall; got != 3 {
		t.Fatalf("publish retry count mismatch: got %d want 3", got)
	}
	if len(bus.published) != 0 {
		t.Fatalf("expected no successful publish after retries exhausted: %+v", bus.published)
	}
	select {
	case err := <-failures:
		if err == nil || err.Error() == "" {
			t.Fatalf("expected concrete publish failure error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected publish failure handler to be invoked")
	}
	if got := stats.Snapshot().RemotePublishFaultTotal; got != 1 {
		t.Fatalf("remote publish fault count mismatch: got %d want 1", got)
	}
}

func testMessage() *entity.Message {
	msg := &entity.Message{
		Id:             1,
		ConversationId: "si_u1_u2",
		Seq:            2,
		ClientMsgId:    "cm1",
		SenderId:       "u1",
		RecvId:         "u2",
		SessionType:    constant.SessionTypeSingle,
		MsgType:        constant.MsgTypeText,
		SendAt:         123,
	}
	msg.SetContent(entity.MessageContent{Text: "hello"})
	return msg
}

func waitDispatchDrained(t *testing.T, coordinator *PushCoordinator) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.WaitDispatchDrained(ctx); err != nil {
		t.Fatalf("wait dispatch drained: %v", err)
	}
}
