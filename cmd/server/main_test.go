package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mbeoliero/kit/log"
	"github.com/mbeoliero/nexo/internal/cluster"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/entity"
	"github.com/mbeoliero/nexo/internal/gateway"
	"github.com/mbeoliero/nexo/pkg/constant"
)

// testGlobalsMu protects global variables modified by tests
var testGlobalsMu sync.Mutex

type stubStatePublisher struct {
	writeCalls int
	syncCalls  int
	writeErr   error
	syncErr    error
}

func (s *stubStatePublisher) WriteState(context.Context, cluster.InstanceState) error {
	s.writeCalls++
	return s.writeErr
}

func (s *stubStatePublisher) Run(context.Context, string, func() cluster.InstanceState) error {
	return nil
}

func (s *stubStatePublisher) SyncNow(context.Context) error {
	s.syncCalls++
	return s.syncErr
}

type blockingSyncStatePublisher struct {
	syncStarted chan struct{}
}

func (s *blockingSyncStatePublisher) WriteState(context.Context, cluster.InstanceState) error {
	return nil
}

func (s *blockingSyncStatePublisher) Run(context.Context, string, func() cluster.InstanceState) error {
	return nil
}

func (s *blockingSyncStatePublisher) SyncNow(ctx context.Context) error {
	select {
	case s.syncStarted <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

type stubDispatchDrainer struct {
	waitCalls   int
	cancelCalls int
	waitErr     error
}

func (s *stubDispatchDrainer) WaitDispatchDrained(context.Context) error {
	s.waitCalls++
	return s.waitErr
}

func (s *stubDispatchDrainer) CancelDispatches() {
	s.cancelCalls++
}

type routeReadFailureLocalDispatcher struct{}

func (s *routeReadFailureLocalDispatcher) DeliverLocal(context.Context, []cluster.ConnRef, *cluster.PushPayload) error {
	return nil
}

func (s *routeReadFailureLocalDispatcher) LocalConnRefs([]string, string) map[string][]cluster.ConnRef {
	return map[string][]cluster.ConnRef{}
}

type routeReadFailureRouteReader struct {
	err error
}

func (s *routeReadFailureRouteReader) GetUsersConnRefs(context.Context, []string) (map[string][]cluster.RouteConn, error) {
	if s.err != nil {
		return nil, s.err
	}
	return map[string][]cluster.RouteConn{}, nil
}

type noOpPushBus struct{}

func (b *noOpPushBus) PublishToInstance(context.Context, string, *cluster.PushEnvelope) error {
	return nil
}

func (b *noOpPushBus) SubscribeInstance(context.Context, string, func(context.Context, *cluster.PushEnvelope), func(), func(error)) error {
	return nil
}

func (b *noOpPushBus) Close() error {
	return nil
}

type stubRouteMirrorOverflowSource struct {
	handler func()
}

func (s *stubRouteMirrorOverflowSource) SetRouteMirrorOverflowHandler(handler func()) {
	s.handler = handler
}

type stubRouteMirrorDepthReader struct {
	mu    sync.Mutex
	depth int
}

func (s *stubRouteMirrorDepthReader) RouteMirrorQueueDepth() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.depth
}

type stubRouteReadProber struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (s *stubRouteReadProber) ProbeRouteRead(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.err
}

type stubRouteMirrorWriteProber struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (s *stubRouteMirrorWriteProber) ProbeRouteMirrorWrite(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.err
}

func captureTestLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	log.SetOutput(buf)
	t.Cleanup(func() {
		log.SetOutput(io.Discard)
	})
	return buf
}

func TestNewInstanceManagerForConfigUsesHeartbeatSecond(t *testing.T) {
	cfg := &config.Config{
		CrossInstance: config.CrossInstanceConfig{
			HeartbeatSecond:         7,
			InstanceAliveTTLSeconds: 30,
		},
	}

	manager := newInstanceManagerForConfig(cfg, nil)

	if got := manager.HeartbeatInterval(); got != 7*time.Second {
		t.Fatalf("heartbeat interval mismatch: got %s want %s", got, 7*time.Second)
	}
}

func TestNewPushCoordinatorForConfigReturnsLocalOnlyCoordinatorWhenDisabled(t *testing.T) {
	cfg := &config.Config{
		CrossInstance: config.CrossInstanceConfig{
			Enabled: false,
		},
	}

	coordinator := newPushCoordinatorForConfig(cfg, "i1", &gateway.WsServer{}, nil, nil, nil)
	if coordinator == nil {
		t.Fatal("expected local-only push coordinator when cross-instance is disabled")
	}
}

func TestDrainSendPathForcesCloseOnDeadline(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	_, err := gate.AcquireSendLease()
	if err != nil {
		t.Fatalf("acquire send lease: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		drainSendPath(ctx, gate, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("drainSendPath did not return after deadline")
	}

	if got := gate.Snapshot().Phase; got != cluster.LifecyclePhaseSendClosed {
		t.Fatalf("phase mismatch: got %s", got)
	}
}

func TestDrainSendPathCancelsDispatchesWhenInflightDrainTimesOut(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	_, err := gate.AcquireSendLease()
	if err != nil {
		t.Fatalf("acquire send lease: %v", err)
	}

	drainer := &stubDispatchDrainer{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	drainSendPath(ctx, gate, drainer)

	if drainer.waitCalls != 0 {
		t.Fatalf("wait dispatch drained should not be called when inflight drain times out: got %d", drainer.waitCalls)
	}
	if drainer.cancelCalls != 1 {
		t.Fatalf("cancel dispatch call count mismatch: got %d want 1", drainer.cancelCalls)
	}
	if got := gate.Snapshot().Phase; got != cluster.LifecyclePhaseSendClosed {
		t.Fatalf("phase mismatch: got %s want %s", got, cluster.LifecyclePhaseSendClosed)
	}
}

func TestVerifyCrossInstanceStartupAcceptsInstanceStatePublisher(t *testing.T) {
	var _ func(
		context.Context,
		string,
		cluster.LifecycleGate,
		routeSnapshotter,
		cluster.InstanceStatePublisher,
		routeReconciler,
	) error = verifyCrossInstanceStartup
}

func TestHandlePushSubscriptionExitSyncsThroughInstanceStatePublisher(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	publisher := &stubStatePublisher{}
	stats := cluster.NewRuntimeStats()

	handlePushSubscriptionExit(context.Background(), errors.New("subscription failed"), gate, publisher, stats)

	snapshot := gate.Snapshot()
	if snapshot.Phase != cluster.LifecyclePhaseDraining {
		t.Fatalf("phase mismatch: got %s", snapshot.Phase)
	}
	if snapshot.Routeable {
		t.Fatal("routeable should be false after subscription runtime failure")
	}
	if publisher.syncCalls != 1 {
		t.Fatalf("sync call count mismatch: got %d want 1", publisher.syncCalls)
	}
	snapshotStats := stats.Snapshot()
	if snapshotStats.SubscriptionFaultTotal != 1 {
		t.Fatalf("subscription fault count mismatch: got %d want 1", snapshotStats.SubscriptionFaultTotal)
	}
	if snapshotStats.LastSubscriptionFaultAt == 0 {
		t.Fatal("expected last subscription fault timestamp to be set")
	}
}

func TestHandleStatePublishRuntimeExitBeginsDrainAndSyncsState(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	publisher := &stubStatePublisher{}
	stats := cluster.NewRuntimeStats()

	handleStatePublishRuntimeExit(context.Background(), errors.New("heartbeat failed"), gate, publisher, stats)

	snapshot := gate.Snapshot()
	if snapshot.Phase != cluster.LifecyclePhaseDraining {
		t.Fatalf("phase mismatch: got %s want %s", snapshot.Phase, cluster.LifecyclePhaseDraining)
	}
	if snapshot.Ready {
		t.Fatal("ready should be false after state publish runtime failure")
	}
	if snapshot.Routeable {
		t.Fatal("routeable should be false after state publish runtime failure")
	}
	if !snapshot.IngressClosed {
		t.Fatal("ingress should be closed after state publish runtime failure")
	}
	if _, err := gate.AcquireSendLease(); err == nil {
		t.Fatal("expected send lease rejection after state publish runtime failure")
	}
	if publisher.syncCalls != 1 {
		t.Fatalf("sync call count mismatch: got %d want 1", publisher.syncCalls)
	}
	if got := stats.Snapshot().StatePublishFaultTotal; got != 1 {
		t.Fatalf("state publish fault count mismatch: got %d want 1", got)
	}
}

func TestHandleReconcileRuntimeExitBeginsDrainAndSyncsState(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	publisher := &stubStatePublisher{}
	stats := cluster.NewRuntimeStats()

	handleReconcileRuntimeExit(context.Background(), errors.New("reconcile failed"), gate, publisher, stats)

	snapshot := gate.Snapshot()
	if snapshot.Phase != cluster.LifecyclePhaseDraining {
		t.Fatalf("phase mismatch: got %s want %s", snapshot.Phase, cluster.LifecyclePhaseDraining)
	}
	if snapshot.Ready {
		t.Fatal("ready should be false after reconcile runtime failure")
	}
	if snapshot.Routeable {
		t.Fatal("routeable should be false after reconcile runtime failure")
	}
	if !snapshot.IngressClosed {
		t.Fatal("ingress should be closed after reconcile runtime failure")
	}
	if _, err := gate.AcquireSendLease(); err == nil {
		t.Fatal("expected send lease rejection after reconcile runtime failure")
	}
	if publisher.syncCalls != 1 {
		t.Fatalf("sync call count mismatch: got %d want 1", publisher.syncCalls)
	}
	if got := stats.Snapshot().ReconcileFaultTotal; got != 1 {
		t.Fatalf("reconcile fault count mismatch: got %d want 1", got)
	}
}

func TestInstallRouteReadFailureHandlerDegradesLifecycleAndSyncsState(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	publisher := &stubStatePublisher{}
	coordinator := cluster.NewPushCoordinator("i1", &routeReadFailureLocalDispatcher{}, &routeReadFailureRouteReader{
		err: errors.New("route read failed"),
	}, &noOpPushBus{})

	installRouteReadFailureHandler(coordinator, gate, publisher)

	coordinator.AsyncPushToUsers(testMessage(), []string{"u1"}, "")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.WaitDispatchDrained(ctx); err != nil {
		t.Fatalf("wait dispatch drained: %v", err)
	}

	snapshot := gate.Snapshot()
	if snapshot.Phase != cluster.LifecyclePhaseDraining {
		t.Fatalf("phase mismatch: got %s want %s", snapshot.Phase, cluster.LifecyclePhaseDraining)
	}
	if snapshot.Ready {
		t.Fatal("ready should be false after route read runtime failure")
	}
	if snapshot.Routeable {
		t.Fatal("routeable should be false after route read runtime failure")
	}
	if !snapshot.IngressClosed {
		t.Fatal("ingress should be closed after route read runtime failure")
	}
	if _, err := gate.AcquireSendLease(); err == nil {
		t.Fatal("expected send lease rejection after route read runtime failure")
	}
	if snapshot.DrainReason != cluster.DrainReasonRouteReadFault {
		t.Fatalf("drain reason mismatch: got %s want %s", snapshot.DrainReason, cluster.DrainReasonRouteReadFault)
	}
	if publisher.syncCalls != 1 {
		t.Fatalf("sync call count mismatch: got %d want 1", publisher.syncCalls)
	}
}

func TestInstallRouteMirrorOverflowHandlerDegradesLifecycleAndSyncsState(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	publisher := &stubStatePublisher{}
	source := &stubRouteMirrorOverflowSource{}

	installRouteMirrorOverflowHandler(source, gate, publisher)
	if source.handler == nil {
		t.Fatal("expected overflow handler to be installed")
	}

	source.handler()

	snapshot := gate.Snapshot()
	if snapshot.Phase != cluster.LifecyclePhaseDraining {
		t.Fatalf("phase mismatch: got %s want %s", snapshot.Phase, cluster.LifecyclePhaseDraining)
	}
	if snapshot.Ready {
		t.Fatal("ready should be false after route mirror overload")
	}
	if snapshot.Routeable {
		t.Fatal("routeable should be false after route mirror overload")
	}
	if !snapshot.IngressClosed {
		t.Fatal("ingress should be closed after route mirror overload")
	}
	if _, err := gate.AcquireSendLease(); err == nil {
		t.Fatal("expected send lease rejection after route mirror overload")
	}
	if snapshot.DrainReason != cluster.DrainReasonRouteMirrorOverload {
		t.Fatalf("drain reason mismatch: got %s want %s", snapshot.DrainReason, cluster.DrainReasonRouteMirrorOverload)
	}
	if publisher.syncCalls != 1 {
		t.Fatalf("sync call count mismatch: got %d want 1", publisher.syncCalls)
	}
}

func TestHandleRouteReadRuntimeExitDoesNotOverrideStatePublishFault(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	if err := gate.MarkDegraded(cluster.DrainReasonStatePublishFault); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}
	publisher := &stubStatePublisher{}

	handleRouteReadRuntimeExit(context.Background(), errors.New("route read failed"), gate, publisher)

	snapshot := gate.Snapshot()
	if snapshot.DrainReason != cluster.DrainReasonStatePublishFault {
		t.Fatalf("drain reason mismatch: got %s want %s", snapshot.DrainReason, cluster.DrainReasonStatePublishFault)
	}
	if snapshot.Ready {
		t.Fatal("ready should remain false")
	}
	if snapshot.Routeable {
		t.Fatal("routeable should remain false")
	}
	if publisher.syncCalls != 0 {
		t.Fatalf("sync call count mismatch: got %d want %d", publisher.syncCalls, 0)
	}
}

func TestHandleRouteMirrorOverflowRuntimeExitDoesNotOverrideReconcileFault(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	if err := gate.MarkDegraded(cluster.DrainReasonReconcileFault); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}
	publisher := &stubStatePublisher{}

	handleRouteMirrorOverflowRuntimeExit(context.Background(), gate, publisher)

	snapshot := gate.Snapshot()
	if snapshot.DrainReason != cluster.DrainReasonReconcileFault {
		t.Fatalf("drain reason mismatch: got %s want %s", snapshot.DrainReason, cluster.DrainReasonReconcileFault)
	}
	if snapshot.Ready {
		t.Fatal("ready should remain false")
	}
	if snapshot.Routeable {
		t.Fatal("routeable should remain false")
	}
	if publisher.syncCalls != 0 {
		t.Fatalf("sync call count mismatch: got %d want %d", publisher.syncCalls, 0)
	}
}

func TestHandleRemotePublishRuntimeExitLeavesLifecycleServing(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	publisher := &stubStatePublisher{}

	handleRemotePublishRuntimeExit(context.Background(), errors.New("remote publish failed"), gate, publisher)

	snapshot := gate.Snapshot()
	if snapshot.Phase != cluster.LifecyclePhaseServing {
		t.Fatalf("phase mismatch: got %s want %s", snapshot.Phase, cluster.LifecyclePhaseServing)
	}
	if !snapshot.Ready {
		t.Fatal("ready should remain true after remote publish fault")
	}
	if !snapshot.Routeable {
		t.Fatal("routeable should remain true after remote publish fault")
	}
	if snapshot.DrainReason != "" {
		t.Fatalf("drain reason mismatch: got %s want empty", snapshot.DrainReason)
	}
	if publisher.syncCalls != 0 {
		t.Fatalf("sync call count mismatch: got %d want 0", publisher.syncCalls)
	}
}

func TestHandleRemotePublishRuntimeExitDoesNotOverrideStatePublishFault(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	if err := gate.MarkDegraded(cluster.DrainReasonStatePublishFault); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}
	publisher := &stubStatePublisher{}

	handleRemotePublishRuntimeExit(context.Background(), errors.New("remote publish failed"), gate, publisher)

	snapshot := gate.Snapshot()
	if snapshot.DrainReason != cluster.DrainReasonStatePublishFault {
		t.Fatalf("drain reason mismatch: got %s want %s", snapshot.DrainReason, cluster.DrainReasonStatePublishFault)
	}
	if snapshot.Ready {
		t.Fatal("ready should remain false")
	}
	if snapshot.Routeable {
		t.Fatal("routeable should remain false")
	}
	if publisher.syncCalls != 0 {
		t.Fatalf("sync call count mismatch: got %d want %d", publisher.syncCalls, 0)
	}
}

func TestRouteabilityRecoveryLoopRestoresHealthyServingState(t *testing.T) {
	testGlobalsMu.Lock()
	defer testGlobalsMu.Unlock()

	oldInterval := routeabilityRecoveryProbeInterval
	oldThreshold := routeabilityRecoverySuccessThreshold
	routeabilityRecoveryProbeInterval = 10 * time.Millisecond
	routeabilityRecoverySuccessThreshold = 2
	defer func() {
		routeabilityRecoveryProbeInterval = oldInterval
		routeabilityRecoverySuccessThreshold = oldThreshold
	}()

	gate := cluster.NewLifecycleGate()
	if err := gate.MarkDegraded(cluster.DrainReasonRouteReadFault); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}
	publisher := &stubStatePublisher{}
	snapshotter := &stubRouteSnapshotter{routes: []cluster.RouteConn{{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}}}
	reconciler := &stubRouteReconciler{}
	depthReader := &stubRouteMirrorDepthReader{}
	readProber := &stubRouteReadProber{}
	writeProber := &stubRouteMirrorWriteProber{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startRouteabilityRecoveryLoop(ctx, gate, publisher, "i1", snapshotter, reconciler, depthReader, readProber, writeProber)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := gate.Snapshot()
		if snapshot.Ready && snapshot.Routeable && snapshot.DrainReason == "" {
			if publisher.syncCalls < 1 {
				t.Fatalf("expected recovery loop to sync state at least once, got %d sync calls", publisher.syncCalls)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("expected recovery loop to restore healthy state, got %+v", gate.Snapshot())
}

func TestRouteabilityRecoveryLoopWaitsForRouteMirrorBacklogToDrain(t *testing.T) {
	testGlobalsMu.Lock()
	defer testGlobalsMu.Unlock()

	oldInterval := routeabilityRecoveryProbeInterval
	oldThreshold := routeabilityRecoverySuccessThreshold
	routeabilityRecoveryProbeInterval = 10 * time.Millisecond
	routeabilityRecoverySuccessThreshold = 1
	defer func() {
		routeabilityRecoveryProbeInterval = oldInterval
		routeabilityRecoverySuccessThreshold = oldThreshold
	}()

	gate := cluster.NewLifecycleGate()
	if err := gate.MarkDegraded(cluster.DrainReasonRouteMirrorOverload); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}
	publisher := &stubStatePublisher{}
	snapshotter := &stubRouteSnapshotter{}
	reconciler := &stubRouteReconciler{}
	depthReader := &stubRouteMirrorDepthReader{depth: 1}
	readProber := &stubRouteReadProber{}
	writeProber := &stubRouteMirrorWriteProber{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startRouteabilityRecoveryLoop(ctx, gate, publisher, "i1", snapshotter, reconciler, depthReader, readProber, writeProber)

	time.Sleep(50 * time.Millisecond)
	snapshot := gate.Snapshot()
	if snapshot.Ready || snapshot.Routeable {
		t.Fatalf("recovery should not happen while backlog is non-zero: %+v", snapshot)
	}

	depthReader.mu.Lock()
	depthReader.depth = 0
	depthReader.mu.Unlock()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot = gate.Snapshot()
		if snapshot.Ready && snapshot.Routeable && snapshot.DrainReason == "" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("expected recovery after backlog drained, got %+v", gate.Snapshot())
}

func TestRouteabilityRecoveryLoopDoesNotRecoverWhileRouteReadProbeStillFails(t *testing.T) {
	testGlobalsMu.Lock()

	oldInterval := routeabilityRecoveryProbeInterval
	oldThreshold := routeabilityRecoverySuccessThreshold
	routeabilityRecoveryProbeInterval = 10 * time.Millisecond
	routeabilityRecoverySuccessThreshold = 1

	gate := cluster.NewLifecycleGate()
	if err := gate.MarkDegraded(cluster.DrainReasonRouteReadFault); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}
	publisher := &stubStatePublisher{}
	snapshotter := &stubRouteSnapshotter{routes: []cluster.RouteConn{{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}}}
	reconciler := &stubRouteReconciler{}
	depthReader := &stubRouteMirrorDepthReader{}
	readProber := &stubRouteReadProber{err: errors.New("route read still failing")}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		startRouteabilityRecoveryLoop(ctx, gate, publisher, "i1", snapshotter, reconciler, depthReader, readProber, &stubRouteMirrorWriteProber{})
		close(done)
	}()

	time.Sleep(80 * time.Millisecond)
	snapshot := gate.Snapshot()

	cancel()
	<-done
	// Goroutine has exited, add small delay to ensure all cleanup is complete
	time.Sleep(10 * time.Millisecond)

	routeabilityRecoveryProbeInterval = oldInterval
	routeabilityRecoverySuccessThreshold = oldThreshold
	testGlobalsMu.Unlock()

	if snapshot.Ready || snapshot.Routeable {
		t.Fatalf("recovery should not happen while read probe fails: %+v", snapshot)
	}
	if snapshot.DrainReason != cluster.DrainReasonRouteReadFault {
		t.Fatalf("drain reason mismatch: got %s want %s", snapshot.DrainReason, cluster.DrainReasonRouteReadFault)
	}
}

func TestRouteabilityRecoveryLoopDoesNotRecoverWhileRouteMirrorWriteProbeFails(t *testing.T) {
	testGlobalsMu.Lock()

	oldInterval := routeabilityRecoveryProbeInterval
	oldThreshold := routeabilityRecoverySuccessThreshold
	routeabilityRecoveryProbeInterval = 10 * time.Millisecond
	routeabilityRecoverySuccessThreshold = 1

	logBuf := captureTestLogs(t)
	gate := cluster.NewLifecycleGate()
	if err := gate.MarkDegraded(cluster.DrainReasonRouteMirrorOverload); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}
	publisher := &stubStatePublisher{}
	snapshotter := &stubRouteSnapshotter{}
	reconciler := &stubRouteReconciler{}
	depthReader := &stubRouteMirrorDepthReader{}
	readProber := &stubRouteReadProber{}
	writeProber := &stubRouteMirrorWriteProber{err: errors.New("mirror write still failing")}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		startRouteabilityRecoveryLoop(ctx, gate, publisher, "i1", snapshotter, reconciler, depthReader, readProber, writeProber)
		close(done)
	}()

	time.Sleep(80 * time.Millisecond)
	snapshot := gate.Snapshot()

	cancel()
	<-done
	// Goroutine has exited, add small delay to ensure all cleanup is complete
	time.Sleep(10 * time.Millisecond)

	routeabilityRecoveryProbeInterval = oldInterval
	routeabilityRecoverySuccessThreshold = oldThreshold
	testGlobalsMu.Unlock()

	if snapshot.Ready || snapshot.Routeable {
		t.Fatalf("recovery should not happen while write probe fails: %+v", snapshot)
	}
	writeProber.mu.Lock()
	calls := writeProber.calls
	writeProber.mu.Unlock()
	if calls == 0 {
		t.Fatal("expected write probe to be attempted")
	}
	readProber.mu.Lock()
	readCalls := readProber.calls
	readProber.mu.Unlock()
	if readCalls != 0 {
		t.Fatalf("expected read probe to stay unused for route_mirror_overload, got %d calls", readCalls)
	}
	// Read log buffer after goroutine has fully exited to avoid race
	if !strings.Contains(logBuf.String(), "probe_type=route_mirror_write") {
		t.Fatalf("expected write-probe log, got %q", logBuf.String())
	}
}

func TestRouteabilityRecoveryLoopKeepsProbeProgressLogsBelowInfo(t *testing.T) {
	testGlobalsMu.Lock()

	oldInterval := routeabilityRecoveryProbeInterval
	oldThreshold := routeabilityRecoverySuccessThreshold
	oldLogLevel := log.GetLogLevel()
	routeabilityRecoveryProbeInterval = 10 * time.Millisecond
	routeabilityRecoverySuccessThreshold = 1
	log.SetLevel(log.LevelInfo)

	logBuf := captureTestLogs(t)
	gate := cluster.NewLifecycleGate()
	if err := gate.MarkDegraded(cluster.DrainReasonRouteReadFault); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}
	publisher := &stubStatePublisher{}
	snapshotter := &stubRouteSnapshotter{routes: []cluster.RouteConn{{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}}}
	reconciler := &stubRouteReconciler{}
	depthReader := &stubRouteMirrorDepthReader{}
	readProber := &stubRouteReadProber{}
	writeProber := &stubRouteMirrorWriteProber{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		startRouteabilityRecoveryLoop(ctx, gate, publisher, "i1", snapshotter, reconciler, depthReader, readProber, writeProber)
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := gate.Snapshot()
		if snapshot.Ready && snapshot.Routeable && snapshot.DrainReason == "" {
			logs := logBuf.String()
			if strings.Contains(logs, "routeability recovery probe start") {
				t.Fatalf("probe start log should stay below info level, got %q", logs)
			}
			if strings.Contains(logs, "routeability recovery probe succeeded") {
				t.Fatalf("probe success log should stay below info level, got %q", logs)
			}
			cancel()
			<-done

			routeabilityRecoveryProbeInterval = oldInterval
			routeabilityRecoverySuccessThreshold = oldThreshold
			log.SetLevel(oldLogLevel)
			testGlobalsMu.Unlock()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	<-done

	routeabilityRecoveryProbeInterval = oldInterval
	routeabilityRecoverySuccessThreshold = oldThreshold
	log.SetLevel(oldLogLevel)
	testGlobalsMu.Unlock()
	t.Fatalf("expected recovery loop to restore healthy state, got %+v", gate.Snapshot())
}

func TestMainHelpersDoNotRequireConcreteInstanceManagerForStateSync(t *testing.T) {
	var _ func(context.Context, cluster.InstanceStatePublisher) = syncLifecycleState

	publisher := &stubStatePublisher{}
	syncLifecycleState(context.Background(), publisher)

	if publisher.syncCalls != 1 {
		t.Fatalf("sync call count mismatch: got %d want 1", publisher.syncCalls)
	}
}

type stubRouteSnapshotter struct {
	routes []cluster.RouteConn
}

func (s *stubRouteSnapshotter) SnapshotRouteConns(_ string) []cluster.RouteConn {
	return append([]cluster.RouteConn(nil), s.routes...)
}

type stubRouteReconciler struct {
	err error
}

type orderedRouteReconciler struct {
	order *[]string
}

func (s *orderedRouteReconciler) ReconcileInstanceRoutes(context.Context, string, []cluster.RouteConn) error {
	*s.order = append(*s.order, "reconcile")
	return nil
}

type orderedStatePublisher struct {
	order *[]string
}

func (s *orderedStatePublisher) WriteState(context.Context, cluster.InstanceState) error {
	*s.order = append(*s.order, "heartbeat")
	return nil
}

func (s *orderedStatePublisher) Run(context.Context, string, func() cluster.InstanceState) error {
	return nil
}

func (s *orderedStatePublisher) SyncNow(context.Context) error {
	return nil
}

type stubReadyPushBus struct {
	readyCh         chan struct{}
	subscribeErr    error
	failBeforeReady bool
}

func (s *stubReadyPushBus) PublishToInstance(context.Context, string, *cluster.PushEnvelope) error {
	return nil
}

func (s *stubReadyPushBus) SubscribeInstance(
	ctx context.Context,
	_ string,
	_ func(context.Context, *cluster.PushEnvelope),
	onReady func(),
	_ func(error),
) error {
	if s.failBeforeReady {
		return s.subscribeErr
	}
	if s.readyCh != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.readyCh:
		}
	}
	if onReady != nil {
		onReady()
	}
	if s.subscribeErr != nil {
		return s.subscribeErr
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *stubReadyPushBus) Close() error { return nil }

type stubOnlineConnCounter struct {
	count int64
}

func (s *stubOnlineConnCounter) GetOnlineConnCount() int64 {
	return s.count
}

func (s *stubRouteReconciler) ReconcileInstanceRoutes(context.Context, string, []cluster.RouteConn) error {
	return s.err
}

func TestVerifyCrossInstanceStartupFailsOnHeartbeatWriteError(t *testing.T) {
	err := verifyCrossInstanceStartup(
		context.Background(),
		"i1",
		cluster.NewLifecycleGate(),
		&stubRouteSnapshotter{},
		&stubStatePublisher{writeErr: errors.New("heartbeat failed")},
		&stubRouteReconciler{},
	)
	if err == nil {
		t.Fatal("expected startup verification to fail on heartbeat write error")
	}
}

func TestVerifyCrossInstanceStartupFailsOnInitialReconcileError(t *testing.T) {
	err := verifyCrossInstanceStartup(
		context.Background(),
		"i1",
		cluster.NewLifecycleGate(),
		&stubRouteSnapshotter{},
		&stubStatePublisher{},
		&stubRouteReconciler{err: errors.New("reconcile failed")},
	)
	if err == nil {
		t.Fatal("expected startup verification to fail on reconcile error")
	}
}

func TestVerifyCrossInstanceStartupRepairsRoutesBeforeAdvertisingAlive(t *testing.T) {
	order := make([]string, 0, 2)

	err := verifyCrossInstanceStartup(
		context.Background(),
		"i1",
		cluster.NewLifecycleGate(),
		&stubRouteSnapshotter{},
		&orderedStatePublisher{order: &order},
		&orderedRouteReconciler{order: &order},
	)
	if err != nil {
		t.Fatalf("verify cross instance startup: %v", err)
	}

	if len(order) != 2 {
		t.Fatalf("startup call count mismatch: %+v", order)
	}
	if order[0] != "reconcile" || order[1] != "heartbeat" {
		t.Fatalf("startup order mismatch: %+v", order)
	}
}

func TestStartPushSubscriptionWaitsForReadyBeforeReturning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus := &stubReadyPushBus{readyCh: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- startPushSubscription(ctx, bus, "i1", func(context.Context, *cluster.PushEnvelope) {}, cluster.NewLifecycleGate(), nil, nil)
	}()

	select {
	case err := <-done:
		t.Fatalf("subscription returned before ready: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(bus.readyCh)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("start push subscription: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("subscription did not report ready")
	}
}

func TestStartPushSubscriptionReturnsInitError(t *testing.T) {
	bus := &stubReadyPushBus{
		subscribeErr:    errors.New("subscribe failed"),
		failBeforeReady: true,
	}

	err := startPushSubscription(context.Background(), bus, "i1", func(context.Context, *cluster.PushEnvelope) {}, cluster.NewLifecycleGate(), nil, nil)
	if !errors.Is(err, bus.subscribeErr) {
		t.Fatalf("init error mismatch: got %v want %v", err, bus.subscribeErr)
	}
}

func TestWaitForRouteableDrainStopsWhenContextExpires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	counter := &stubOnlineConnCounter{count: 1}
	start := time.Now()
	waitForRouteableDrain(ctx, counter, time.Second, time.Now, func(d time.Duration) {
		time.Sleep(10 * time.Millisecond)
	})
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("routeable drain ignored context deadline: %s", elapsed)
	}
}

func TestInstallRemoteOverflowHandlerBeginsDrainAndCancelsSubscription(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	publisher := &stubStatePublisher{}
	stats := cluster.NewRuntimeStats()
	local := &overflowTestLocalDispatcher{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	coordinator := cluster.NewPushCoordinator("i1", local, nil, nil)
	coordinator.ConfigureDispatch(1, 1, 500)

	cancelled := make(chan struct{}, 1)
	installRemoteOverflowHandler(coordinator, gate, publisher, stats, func() {
		cancelled <- struct{}{}
	})

	first := &cluster.PushEnvelope{
		Version: 1,
		TargetConnMap: map[string][]cluster.ConnRef{
			"i1": {{UserId: "u1", ConnId: "c1", PlatformId: 5}},
		},
		Payload: &cluster.PushPayload{Message: &cluster.PushMessage{ConversationId: "c1"}},
	}
	second := &cluster.PushEnvelope{
		Version: 1,
		TargetConnMap: map[string][]cluster.ConnRef{
			"i1": {{UserId: "u2", ConnId: "c2", PlatformId: 1}},
		},
		Payload: &cluster.PushPayload{Message: &cluster.PushMessage{ConversationId: "c1"}},
	}
	third := &cluster.PushEnvelope{
		Version: 1,
		TargetConnMap: map[string][]cluster.ConnRef{
			"i1": {{UserId: "u3", ConnId: "c3", PlatformId: 2}},
		},
		Payload: &cluster.PushPayload{Message: &cluster.PushMessage{ConversationId: "c1"}},
	}

	coordinator.EnqueueRemoteEnvelope(context.Background(), first)

	select {
	case <-local.started:
	case <-time.After(time.Second):
		t.Fatal("remote worker did not start")
	}

	coordinator.EnqueueRemoteEnvelope(context.Background(), second)
	coordinator.EnqueueRemoteEnvelope(context.Background(), third)

	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("expected subscription cancel on remote overflow")
	}

	snapshot := gate.Snapshot()
	if snapshot.Phase != cluster.LifecyclePhaseDraining {
		t.Fatalf("phase mismatch: got %s", snapshot.Phase)
	}
	if snapshot.Routeable {
		t.Fatal("routeable should be false after remote overflow")
	}
	if publisher.syncCalls != 1 {
		t.Fatalf("sync call count mismatch: got %d want 1", publisher.syncCalls)
	}
	if got := stats.Snapshot().SubscriptionFaultTotal; got != 1 {
		t.Fatalf("subscription fault count mismatch: got %d want 1", got)
	}

	close(local.release)
}

func TestInstallRemoteOverflowHandlerCancelsSubscriptionAfterTimedStateSync(t *testing.T) {
	oldTimeout := runtimeFaultSyncTimeout
	runtimeFaultSyncTimeout = 20 * time.Millisecond
	defer func() {
		runtimeFaultSyncTimeout = oldTimeout
	}()

	gate := cluster.NewLifecycleGate()
	publisher := &blockingSyncStatePublisher{syncStarted: make(chan struct{}, 1)}
	local := &overflowTestLocalDispatcher{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	coordinator := cluster.NewPushCoordinator("i1", local, nil, nil)
	coordinator.ConfigureDispatch(1, 1, 500)

	cancelled := make(chan struct{}, 1)
	installRemoteOverflowHandler(coordinator, gate, publisher, nil, func() {
		cancelled <- struct{}{}
	})

	first := &cluster.PushEnvelope{
		Version: 1,
		TargetConnMap: map[string][]cluster.ConnRef{
			"i1": {{UserId: "u1", ConnId: "c1", PlatformId: 5}},
		},
		Payload: &cluster.PushPayload{Message: &cluster.PushMessage{ConversationId: "c1"}},
	}
	second := &cluster.PushEnvelope{
		Version: 1,
		TargetConnMap: map[string][]cluster.ConnRef{
			"i1": {{UserId: "u2", ConnId: "c2", PlatformId: 1}},
		},
		Payload: &cluster.PushPayload{Message: &cluster.PushMessage{ConversationId: "c1"}},
	}
	third := &cluster.PushEnvelope{
		Version: 1,
		TargetConnMap: map[string][]cluster.ConnRef{
			"i1": {{UserId: "u3", ConnId: "c3", PlatformId: 2}},
		},
		Payload: &cluster.PushPayload{Message: &cluster.PushMessage{ConversationId: "c1"}},
	}

	coordinator.EnqueueRemoteEnvelope(context.Background(), first)

	select {
	case <-local.started:
	case <-time.After(time.Second):
		t.Fatal("remote worker did not start")
	}

	coordinator.EnqueueRemoteEnvelope(context.Background(), second)
	coordinator.EnqueueRemoteEnvelope(context.Background(), third)

	select {
	case <-publisher.syncStarted:
	case <-time.After(time.Second):
		t.Fatal("expected state sync to start")
	}

	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("expected subscription cancel after timed sync")
	}

	close(local.release)
}

type overflowTestLocalDispatcher struct {
	started chan struct{}
	release chan struct{}
}

func (s *overflowTestLocalDispatcher) DeliverLocal(context.Context, []cluster.ConnRef, *cluster.PushPayload) error {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-s.release
	return nil
}

func (s *overflowTestLocalDispatcher) LocalConnRefs([]string, string) map[string][]cluster.ConnRef {
	return map[string][]cluster.ConnRef{}
}

func TestRouteabilityRecoveryLoopRestoresHealthyStateFromStatePublishFault(t *testing.T) {
	testGlobalsMu.Lock()
	defer testGlobalsMu.Unlock()

	oldInterval := routeabilityRecoveryProbeInterval
	oldThreshold := routeabilityRecoverySuccessThreshold
	routeabilityRecoveryProbeInterval = 10 * time.Millisecond
	routeabilityRecoverySuccessThreshold = 2
	defer func() {
		routeabilityRecoveryProbeInterval = oldInterval
		routeabilityRecoverySuccessThreshold = oldThreshold
	}()

	gate := cluster.NewLifecycleGate()
	if err := gate.MarkDegraded(cluster.DrainReasonStatePublishFault); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}
	publisher := &stubStatePublisher{}
	snapshotter := &stubRouteSnapshotter{routes: []cluster.RouteConn{{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}}}
	reconciler := &stubRouteReconciler{}
	depthReader := &stubRouteMirrorDepthReader{}
	readProber := &stubRouteReadProber{}
	writeProber := &stubRouteMirrorWriteProber{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startRouteabilityRecoveryLoop(ctx, gate, publisher, "i1", snapshotter, reconciler, depthReader, readProber, writeProber)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := gate.Snapshot()
		if snapshot.Ready && snapshot.Routeable && snapshot.DrainReason == "" {
			if publisher.syncCalls < 1 {
				t.Fatalf("expected recovery loop to sync state at least once, got %d sync calls", publisher.syncCalls)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("expected recovery loop to restore healthy state from state publish fault, got %+v", gate.Snapshot())
}

func TestRouteabilityRecoveryLoopRestoresHealthyStateFromReconcileFault(t *testing.T) {
	testGlobalsMu.Lock()
	defer testGlobalsMu.Unlock()

	oldInterval := routeabilityRecoveryProbeInterval
	oldThreshold := routeabilityRecoverySuccessThreshold
	routeabilityRecoveryProbeInterval = 10 * time.Millisecond
	routeabilityRecoverySuccessThreshold = 2
	defer func() {
		routeabilityRecoveryProbeInterval = oldInterval
		routeabilityRecoverySuccessThreshold = oldThreshold
	}()

	gate := cluster.NewLifecycleGate()
	if err := gate.MarkDegraded(cluster.DrainReasonReconcileFault); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}
	publisher := &stubStatePublisher{}
	snapshotter := &stubRouteSnapshotter{routes: []cluster.RouteConn{{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}}}
	reconciler := &stubRouteReconciler{}
	depthReader := &stubRouteMirrorDepthReader{}
	readProber := &stubRouteReadProber{}
	writeProber := &stubRouteMirrorWriteProber{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startRouteabilityRecoveryLoop(ctx, gate, publisher, "i1", snapshotter, reconciler, depthReader, readProber, writeProber)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := gate.Snapshot()
		if snapshot.Ready && snapshot.Routeable && snapshot.DrainReason == "" {
			if publisher.syncCalls < 1 {
				t.Fatalf("expected recovery loop to sync state at least once, got %d sync calls", publisher.syncCalls)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("expected recovery loop to restore healthy state from reconcile fault, got %+v", gate.Snapshot())
}

func TestRouteabilityRecoveryLoopDoesNotRecoverWhileStatePublishProbeStillFails(t *testing.T) {
	testGlobalsMu.Lock()

	oldInterval := routeabilityRecoveryProbeInterval
	oldThreshold := routeabilityRecoverySuccessThreshold
	routeabilityRecoveryProbeInterval = 10 * time.Millisecond
	routeabilityRecoverySuccessThreshold = 1

	gate := cluster.NewLifecycleGate()
	if err := gate.MarkDegraded(cluster.DrainReasonStatePublishFault); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}
	publisher := &stubStatePublisher{syncErr: errors.New("state publish still failing")}
	snapshotter := &stubRouteSnapshotter{routes: []cluster.RouteConn{{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}}}
	reconciler := &stubRouteReconciler{}
	depthReader := &stubRouteMirrorDepthReader{}
	readProber := &stubRouteReadProber{}
	writeProber := &stubRouteMirrorWriteProber{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		startRouteabilityRecoveryLoop(ctx, gate, publisher, "i1", snapshotter, reconciler, depthReader, readProber, writeProber)
		close(done)
	}()

	time.Sleep(80 * time.Millisecond)
	snapshot := gate.Snapshot()

	cancel()
	<-done
	// Goroutine has exited, add small delay to ensure all cleanup is complete
	time.Sleep(10 * time.Millisecond)

	routeabilityRecoveryProbeInterval = oldInterval
	routeabilityRecoverySuccessThreshold = oldThreshold
	testGlobalsMu.Unlock()

	if snapshot.Ready || snapshot.Routeable {
		t.Fatalf("recovery should not happen while state publish probe fails: %+v", snapshot)
	}
	if snapshot.DrainReason != cluster.DrainReasonStatePublishFault {
		t.Fatalf("drain reason mismatch: got %s want %s", snapshot.DrainReason, cluster.DrainReasonStatePublishFault)
	}
}

func TestRouteabilityRecoveryLoopDoesNotRecoverWhileReconcileProbeFails(t *testing.T) {
	testGlobalsMu.Lock()

	oldInterval := routeabilityRecoveryProbeInterval
	oldThreshold := routeabilityRecoverySuccessThreshold
	routeabilityRecoveryProbeInterval = 10 * time.Millisecond
	routeabilityRecoverySuccessThreshold = 1

	gate := cluster.NewLifecycleGate()
	if err := gate.MarkDegraded(cluster.DrainReasonReconcileFault); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}
	publisher := &stubStatePublisher{}
	snapshotter := &stubRouteSnapshotter{routes: []cluster.RouteConn{{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}}}
	reconciler := &stubRouteReconciler{err: errors.New("reconcile still failing")}
	depthReader := &stubRouteMirrorDepthReader{}
	readProber := &stubRouteReadProber{}
	writeProber := &stubRouteMirrorWriteProber{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		startRouteabilityRecoveryLoop(ctx, gate, publisher, "i1", snapshotter, reconciler, depthReader, readProber, writeProber)
		close(done)
	}()

	time.Sleep(80 * time.Millisecond)
	snapshot := gate.Snapshot()

	cancel()
	<-done
	// Goroutine has exited, add small delay to ensure all cleanup is complete
	time.Sleep(10 * time.Millisecond)

	routeabilityRecoveryProbeInterval = oldInterval
	routeabilityRecoverySuccessThreshold = oldThreshold
	testGlobalsMu.Unlock()

	if snapshot.Ready || snapshot.Routeable {
		t.Fatalf("recovery should not happen while reconcile probe fails: %+v", snapshot)
	}
	if snapshot.DrainReason != cluster.DrainReasonReconcileFault {
		t.Fatalf("drain reason mismatch: got %s want %s", snapshot.DrainReason, cluster.DrainReasonReconcileFault)
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
