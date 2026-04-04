package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/google/uuid"
	"github.com/mbeoliero/kit/log"
	"github.com/redis/go-redis/v9"

	"github.com/mbeoliero/nexo/internal/cluster"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/gateway"
	"github.com/mbeoliero/nexo/internal/handler"
	"github.com/mbeoliero/nexo/internal/middleware"
	"github.com/mbeoliero/nexo/internal/repository"
	"github.com/mbeoliero/nexo/internal/router"
	"github.com/mbeoliero/nexo/internal/service"
	"github.com/mbeoliero/nexo/pkg/constant"
)

var runtimeFaultSyncTimeout = 2 * time.Second
var routeabilityRecoveryProbeInterval = time.Second
var routeabilityRecoverySuccessThreshold = 3

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load configuration
	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		log.CtxError(ctx, "failed to load config: %v", err)
		panic(err)
	}

	log.CtxInfo(ctx, "config loaded: mode=%s", cfg.Server.Mode)

	// Initialize Redis key prefix
	constant.InitRedisKeyPrefix(cfg.Redis.KeyPrefix)
	log.CtxInfo(ctx, "redis key prefix: %s", constant.GetRedisKeyPrefix())

	// Initialize repositories
	repos, err := repository.NewRepositories(cfg)
	if err != nil {
		log.CtxError(ctx, "failed to initialize repositories: %v", err)
		panic(err)
	}
	defer func() { _ = repos.Close() }()

	// Check database connection
	if err = repos.CheckConnection(ctx); err != nil {
		log.CtxError(ctx, "database connection check failed: %v", err)
		panic(err)
	}
	log.CtxInfo(ctx, "database connection established")

	// Initialize services
	authService := service.NewAuthService(repos.User, cfg, repos.Redis)
	middleware.SetNativeTokenStateValidator(authService.ValidateIssuedToken)
	userService := service.NewUserService(repos.User)
	groupService := service.NewGroupService(repos)
	msgService := service.NewMessageService(repos)
	convService := service.NewConversationService(repos)
	lifecycleGate := cluster.NewLifecycleGate()

	// Initialize WebSocket server
	wsServer := gateway.NewWsServer(cfg, repos.Redis, msgService, convService)
	wsServer.SetLifecycleGate(lifecycleGate)

	// Set message pusher for message service
	msgService.SetPusher(wsServer)

	instanceId := cfg.CrossInstance.InstanceId
	if instanceId == "" {
		instanceId = uuid.NewString()
	}

	var presenceService *service.PresenceService
	var stateSyncer cluster.InstanceStatePublisher
	var runtimeStats *cluster.RuntimeStats
	if cfg.CrossInstance.Enabled {
		runtimeStats = cluster.NewRuntimeStats()
	}
	pushCoordinator := newPushCoordinatorForConfig(cfg, instanceId, wsServer, nil, nil, runtimeStats)
	wsServer.SetRuntimeStats(runtimeStats)
	wsRuntimeCtx, wsRuntimeCancel := context.WithCancel(ctx)
	subscribeCtx, subscribeCancel := context.WithCancel(ctx)
	reconcileCtx, reconcileCancel := context.WithCancel(ctx)
	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	recoveryCtx, recoveryCancel := context.WithCancel(ctx)
	defer wsRuntimeCancel()
	defer subscribeCancel()
	defer reconcileCancel()
	defer heartbeatCancel()
	defer recoveryCancel()

	if cfg.CrossInstance.Enabled {
		log.CtxInfo(ctx,
			"cross-instance mode enabled: instance_id=%s dispatch_queue_size=%d dispatch_workers=%d subscription_channel_size=%d max_refs_per_envelope=%d",
			instanceId,
			cfg.WebSocket.PushChannelSize,
			cfg.WebSocket.PushWorkerNum,
			cfg.CrossInstance.SubscriptionChannelSize,
			cfg.CrossInstance.MaxRefsPerEnvelope,
		)

		instanceManager := newInstanceManagerForConfig(cfg, repos.Redis)
		routeStore := cluster.NewRouteStore(repos.Redis, time.Duration(cfg.CrossInstance.RouteTTLSeconds)*time.Second, instanceManager)
		pushBus := cluster.NewRedisPushBus(repos.Redis, cluster.PushSubscriptionConfig{
			ChannelSize: cfg.CrossInstance.SubscriptionChannelSize,
			SendTimeout: cfg.CrossInstance.SubscriptionSendTimeout,
		})
		pushCoordinator = newPushCoordinatorForConfig(cfg, instanceId, wsServer, routeStore, pushBus, runtimeStats)
		stateSyncer = instanceManager
		installRemoteOverflowHandler(pushCoordinator, lifecycleGate, stateSyncer, runtimeStats, subscribeCancel)
		installRouteMirrorOverflowHandler(wsServer, lifecycleGate, stateSyncer)
		installRouteReadFailureHandler(pushCoordinator, lifecycleGate, stateSyncer)
		installRemotePublishFailureHandler(pushCoordinator, lifecycleGate, stateSyncer)

		wsServer.SetRouteStore(instanceId, routeStore)
		stateSnapshot := func() cluster.InstanceState {
			snapshot := lifecycleGate.Snapshot()
			return cluster.InstanceState{
				InstanceId: instanceId,
				Alive:      boolPtr(snapshot.Phase != cluster.LifecyclePhaseStopped),
				Ready:      snapshot.Ready,
				Routeable:  snapshot.Routeable,
				Draining:   snapshot.Phase == cluster.LifecyclePhaseDraining,
			}
		}
		instanceManager.ConfigureSnapshot(instanceId, stateSnapshot)
		startRouteabilityRecoveryLoop(recoveryCtx, lifecycleGate, stateSyncer, instanceId, wsServer, routeStore, wsServer, routeStore, wsServer)
		if err := startPushSubscription(subscribeCtx, pushBus, instanceId, pushCoordinator.EnqueueRemoteEnvelope, lifecycleGate, stateSyncer, runtimeStats); err != nil {
			log.CtxError(ctx, "push subscribe startup failed: %v", err)
			panic(err)
		}
		if err := verifyCrossInstanceStartup(ctx, instanceId, lifecycleGate, wsServer, instanceManager, routeStore); err != nil {
			log.CtxError(ctx, "cross-instance startup verification failed: %v", err)
			panic(err)
		}
		log.CtxInfo(ctx, "cross-instance startup complete: instance_id=%s", instanceId)

		go func() {
			if err := instanceManager.Run(heartbeatCtx, instanceId, stateSnapshot); err != nil && !errors.Is(err, context.Canceled) {
				log.CtxError(ctx, "instance heartbeat stopped: %v", err)
				handleStatePublishRuntimeExit(ctx, err, lifecycleGate, stateSyncer, runtimeStats)
			}
		}()
		go func() {
			if err := cluster.RunRouteReconcileLoop(
				reconcileCtx,
				time.Duration(cfg.CrossInstance.ReconcileIntervalSeconds)*time.Second,
				instanceId,
				wsServer,
				routeStore,
				runtimeStats,
			); err != nil && !errors.Is(err, context.Canceled) {
				log.CtxError(ctx, "route reconcile stopped: %v", err)
				handleReconcileRuntimeExit(ctx, err, lifecycleGate, stateSyncer, runtimeStats)
			}
		}()

		presenceService = service.NewPresenceService(instanceId, wsServer, cluster.NewRouteStorePresenceReader(routeStore)).SetStats(runtimeStats)
	} else {
		presenceService = service.NewPresenceService(instanceId, wsServer, nil)
	}
	wsServer.SetPushDelegate(pushCoordinator)

	// Start WebSocket server
	wsServer.Run(wsRuntimeCtx)
	log.CtxInfo(ctx, "websocket server started")

	// Initialize handlers
	handlers := &router.Handlers{
		Auth:         handler.NewAuthHandler(authService),
		User:         handler.NewUserHandler(userService, presenceService),
		Group:        handler.NewGroupHandler(groupService),
		Message:      handler.NewMessageHandler(msgService, lifecycleGate),
		Conversation: handler.NewConversationHandler(convService),
	}

	// Create Hertz server
	h := server.Default(
		server.WithHostPorts(fmt.Sprintf(":%d", cfg.Server.HTTPPort)),
	)

	// Setup routes
	router.SetupRouter(h, handlers, wsServer, lifecycleGate, cfg.CrossInstance.Enabled, cfg.Server.ReadyDetailEnabled, runtimeStats)

	log.CtxInfo(ctx, "server starting on port %d", cfg.Server.HTTPPort)

	// Start server in goroutine
	go func() {
		h.Spin()
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.CtxInfo(ctx, "shutting down server...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.CrossInstance.DrainTimeoutSeconds)*time.Second)
	defer shutdownCancel()

	_ = lifecycleGate.BeginDrain(cluster.DrainReasonPlanned)
	syncLifecycleState(shutdownCtx, stateSyncer)

	// Graceful shutdown
	if err = h.Shutdown(shutdownCtx); err != nil {
		log.CtxError(ctx, "server shutdown error: %v", err)
	}

	waitForRouteableDrain(
		shutdownCtx,
		wsServer,
		time.Duration(cfg.CrossInstance.DrainRouteableGraceSeconds)*time.Second,
		time.Now,
		time.Sleep,
	)
	log.CtxInfo(shutdownCtx, "routeable drain window complete: instance_id=%s remaining_online_conns=%d", instanceId, wsServer.GetOnlineConnCount())
	_ = lifecycleGate.MarkRouteableOff()
	syncLifecycleState(shutdownCtx, stateSyncer)

	drainSendPath(shutdownCtx, lifecycleGate, pushCoordinator)
	syncLifecycleState(shutdownCtx, stateSyncer)

	subscribeCancel()
	wsRuntimeCancel()
	reconcileCancel()
	recoveryCancel()
	_ = lifecycleGate.MarkStopped()
	syncLifecycleState(shutdownCtx, stateSyncer)
	heartbeatCancel()

	log.CtxInfo(ctx, "server stopped")
}

type dispatchDrainer interface {
	WaitDispatchDrained(ctx context.Context) error
}

type dispatchCanceler interface {
	CancelDispatches()
}

type onlineConnCounter interface {
	GetOnlineConnCount() int64
}

type routeSnapshotter interface {
	SnapshotRouteConns(instanceId string) []cluster.RouteConn
}

type routeReconciler interface {
	ReconcileInstanceRoutes(ctx context.Context, instanceId string, want []cluster.RouteConn) error
}

type routeReadProber interface {
	ProbeRouteRead(ctx context.Context, instanceId string) error
}

type routeMirrorOverflowSource interface {
	SetRouteMirrorOverflowHandler(func())
}

type routeMirrorDepthReader interface {
	RouteMirrorQueueDepth() int
}

type routeMirrorWriteProber interface {
	ProbeRouteMirrorWrite(ctx context.Context) error
}

func newInstanceManagerForConfig(cfg *config.Config, rdb *redis.Client) *cluster.InstanceManager {
	manager := cluster.NewInstanceManager(rdb, time.Duration(cfg.CrossInstance.InstanceAliveTTLSeconds)*time.Second)
	if cfg.CrossInstance.HeartbeatSecond > 0 {
		manager.SetHeartbeatInterval(time.Duration(cfg.CrossInstance.HeartbeatSecond) * time.Second)
	}
	return manager
}

func newPushCoordinatorForConfig(
	cfg *config.Config,
	instanceId string,
	wsServer *gateway.WsServer,
	routeReader cluster.RemoteRouteReader,
	pushBus cluster.PushBus,
	stats *cluster.RuntimeStats,
) *cluster.PushCoordinator {
	coordinator := cluster.NewPushCoordinator(instanceId, wsServer, routeReader, pushBus)
	coordinator.ConfigureDispatch(
		cfg.WebSocket.PushChannelSize,
		cfg.WebSocket.PushWorkerNum,
		cfg.CrossInstance.MaxRefsPerEnvelope,
	)
	coordinator.SetRuntimeStats(stats)

	if !cfg.CrossInstance.Enabled {
		return coordinator
	}
	return coordinator
}

// drainSendPath closes the realtime send path in three steps: wait for inflight
// request work, wait for locally accepted async dispatch backlog, then close the
// lifecycle gate. If the shared drain deadline expires, it force-closes the send
// path and cancels local async dispatches.
func drainSendPath(ctx context.Context, gate cluster.LifecycleGate, drainer dispatchDrainer) {
	if gate == nil {
		return
	}

	log.CtxInfo(ctx, "draining send path: wait inflight requests")
	if err := gate.WaitInflightZero(ctx); err != nil {
		if shouldForceCloseSendPath(err) {
			if canceler, ok := drainer.(dispatchCanceler); ok {
				canceler.CancelDispatches()
			}
			log.CtxWarn(ctx, "force closing send path after inflight drain deadline: %v", err)
			_ = gate.ForceCloseSendPath()
			return
		}
		log.CtxError(ctx, "wait inflight sends failed: %v", err)
		return
	}

	if drainer != nil {
		log.CtxInfo(ctx, "draining send path: wait accepted async dispatch")
		if err := drainer.WaitDispatchDrained(ctx); err != nil {
			if shouldForceCloseSendPath(err) {
				if canceler, ok := drainer.(dispatchCanceler); ok {
					canceler.CancelDispatches()
				}
				log.CtxWarn(ctx, "force closing send path after dispatch drain deadline: %v", err)
				_ = gate.ForceCloseSendPath()
				return
			}
			log.CtxError(ctx, "wait dispatch drain failed: %v", err)
			return
		}
	}

	if err := gate.CloseSendPath(); err != nil {
		log.CtxError(ctx, "close send path failed: %v", err)
		return
	}
	log.CtxInfo(ctx, "send path closed")
}

func shouldForceCloseSendPath(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// handlePushSubscriptionExit converts a subscription failure into a drain signal
// and persists the degraded lifecycle state for peer routing decisions.
func handlePushSubscriptionExit(ctx context.Context, err error, gate cluster.LifecycleGate, syncer cluster.InstanceStatePublisher, stats *cluster.RuntimeStats) {
	if err == nil || gate == nil || errors.Is(err, context.Canceled) {
		return
	}
	log.CtxWarn(ctx, "push subscription fault, begin draining: %v", err)
	if stats != nil {
		stats.RecordSubscriptionFault(time.Now())
	}
	_ = gate.BeginDrain(cluster.DrainReasonSubscribeFault)
	syncLifecycleState(ctx, syncer)
}

func handleStatePublishRuntimeExit(ctx context.Context, err error, gate cluster.LifecycleGate, syncer cluster.InstanceStatePublisher, stats *cluster.RuntimeStats) {
	if err == nil || gate == nil || errors.Is(err, context.Canceled) {
		return
	}
	log.CtxWarn(ctx, "instance state publish fault, degrade routing/readiness: %v", err)
	if stats != nil {
		stats.RecordStatePublishFault()
	}
	_ = gate.MarkDegraded(cluster.DrainReasonStatePublishFault)
	syncLifecycleState(ctx, syncer)
}

func handleReconcileRuntimeExit(ctx context.Context, err error, gate cluster.LifecycleGate, syncer cluster.InstanceStatePublisher, stats *cluster.RuntimeStats) {
	if err == nil || gate == nil || errors.Is(err, context.Canceled) {
		return
	}
	log.CtxWarn(ctx, "route reconcile fault, degrade routing/readiness: %v", err)
	if stats != nil {
		stats.RecordReconcileFault()
	}
	_ = gate.MarkDegraded(cluster.DrainReasonReconcileFault)
	syncLifecycleState(ctx, syncer)
}

func handleRouteReadRuntimeExit(ctx context.Context, err error, gate cluster.LifecycleGate, syncer cluster.InstanceStatePublisher) {
	if err == nil || gate == nil || errors.Is(err, context.Canceled) {
		return
	}
	if shouldSkipRecoverableFaultTransition(gate, cluster.DrainReasonRouteReadFault) {
		return
	}
	log.CtxWarn(ctx, "route read fault, degrade routing/readiness: %v", err)
	_ = gate.MarkDegraded(cluster.DrainReasonRouteReadFault)
	syncLifecycleState(ctx, syncer)
}

func handleRouteMirrorOverflowRuntimeExit(ctx context.Context, gate cluster.LifecycleGate, syncer cluster.InstanceStatePublisher) {
	if gate == nil {
		return
	}
	if shouldSkipRecoverableFaultTransition(gate, cluster.DrainReasonRouteMirrorOverload) {
		return
	}
	log.CtxWarn(ctx, "route mirror overload, degrade routing/readiness")
	_ = gate.MarkDegraded(cluster.DrainReasonRouteMirrorOverload)
	syncLifecycleState(ctx, syncer)
}

// handleRemotePublishRuntimeExit logs remote publish failures but does not degrade lifecycle state.
// Unlike other failure handlers (route read, subscription, mirror overload), remote publish failures
// do not prevent this instance from accepting new connections or being routed to. The failure only
// affects outbound message delivery to remote instances, which can be retried or handled by the
// remote instance's own recovery mechanisms. Degrading lifecycle state would unnecessarily remove
// this instance from the routing pool when it can still serve local connections and receive messages.
func handleRemotePublishRuntimeExit(ctx context.Context, err error, gate cluster.LifecycleGate, syncer cluster.InstanceStatePublisher) {
	if err == nil || gate == nil || errors.Is(err, context.Canceled) {
		return
	}
	log.CtxWarn(ctx, "remote publish fault, keeping lifecycle serving: %v", err)
}

func shouldSkipRecoverableFaultTransition(gate cluster.LifecycleGate, reason cluster.DrainReason) bool {
	if gate == nil {
		return false
	}
	snapshot := gate.Snapshot()
	if snapshot.Phase != cluster.LifecyclePhaseServing {
		return true
	}
	if !snapshot.Ready && !snapshot.Routeable {
		if snapshot.DrainReason == reason {
			return true
		}
		switch snapshot.DrainReason {
		case cluster.DrainReasonStatePublishFault, cluster.DrainReasonReconcileFault:
			return true
		}
	}
	return false
}

// installRemoteOverflowHandler wires remote queue overflow to the same
// subscription-fault drain path so peers stop routing traffic here.
func installRemoteOverflowHandler(
	coordinator *cluster.PushCoordinator,
	gate cluster.LifecycleGate,
	syncer cluster.InstanceStatePublisher,
	stats *cluster.RuntimeStats,
	cancel func(),
) {
	if coordinator == nil {
		return
	}
	coordinator.SetRemoteOverflowHandler(func() {
		faultCtx, cancelFault := context.WithTimeout(context.Background(), runtimeFaultSyncTimeout)
		defer cancelFault()
		log.CtxWarn(faultCtx, "remote envelope queue overflow detected, canceling subscription")
		handlePushSubscriptionExit(faultCtx, cluster.ErrRemoteEnvelopeQueueFull, gate, syncer, stats)
		if cancel != nil {
			cancel()
		}
	})
}

func installRouteMirrorOverflowHandler(
	source routeMirrorOverflowSource,
	gate cluster.LifecycleGate,
	syncer cluster.InstanceStatePublisher,
) {
	if source == nil {
		return
	}
	source.SetRouteMirrorOverflowHandler(func() {
		faultCtx, cancelFault := context.WithTimeout(context.Background(), runtimeFaultSyncTimeout)
		defer cancelFault()
		handleRouteMirrorOverflowRuntimeExit(faultCtx, gate, syncer)
	})
}

func installRouteReadFailureHandler(
	coordinator *cluster.PushCoordinator,
	gate cluster.LifecycleGate,
	syncer cluster.InstanceStatePublisher,
) {
	if coordinator == nil {
		return
	}
	coordinator.SetRouteReadFailureHandler(func(err error) {
		faultCtx, cancelFault := context.WithTimeout(context.Background(), runtimeFaultSyncTimeout)
		defer cancelFault()
		handleRouteReadRuntimeExit(faultCtx, err, gate, syncer)
	})
}

func installRemotePublishFailureHandler(
	coordinator *cluster.PushCoordinator,
	gate cluster.LifecycleGate,
	syncer cluster.InstanceStatePublisher,
) {
	if coordinator == nil {
		return
	}
	coordinator.SetRemotePublishFailureHandler(func(err error) {
		faultCtx, cancelFault := context.WithTimeout(context.Background(), runtimeFaultSyncTimeout)
		defer cancelFault()
		handleRemotePublishRuntimeExit(faultCtx, err, gate, syncer)
	})
}

func startRouteabilityRecoveryLoop(
	ctx context.Context,
	gate cluster.LifecycleGate,
	syncer cluster.InstanceStatePublisher,
	instanceID string,
	snapshotter routeSnapshotter,
	reconciler routeReconciler,
	depthReader routeMirrorDepthReader,
	readProber routeReadProber,
	writeProber routeMirrorWriteProber,
) {
	if ctx == nil || gate == nil || syncer == nil || instanceID == "" || snapshotter == nil || reconciler == nil || readProber == nil || writeProber == nil {
		return
	}
	go func() {
		// Capture global variables at goroutine start to avoid data races with tests
		probeInterval := routeabilityRecoveryProbeInterval
		successThreshold := routeabilityRecoverySuccessThreshold

		ticker := time.NewTicker(probeInterval)
		defer ticker.Stop()

		successCount := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			snapshot := gate.Snapshot()
			if !shouldAttemptRouteabilityRecovery(snapshot) {
				successCount = 0
				continue
			}
			queueDepth := 0
			if depthReader != nil {
				queueDepth = depthReader.RouteMirrorQueueDepth()
			}
			if queueDepth > 0 {
				successCount = 0
				continue
			}

			probeType := routeabilityProbeType(snapshot.DrainReason)
			log.CtxDebug(ctx,
				"routeability recovery probe start: instance_id=%s drain_reason=%s probe_type=%s success_count=%d threshold=%d queue_depth=%d",
				instanceID,
				snapshot.DrainReason,
				probeType,
				successCount,
				successThreshold,
				queueDepth,
			)
			probeCtx, cancelProbe := context.WithTimeout(ctx, runtimeFaultSyncTimeout)
			err := probeRouteabilityRecovery(probeCtx, syncer, snapshot, instanceID, snapshotter, reconciler, readProber, writeProber)
			cancelProbe()
			if err != nil {
				log.CtxWarn(ctx,
					"routeability recovery probe failed: instance_id=%s drain_reason=%s probe_type=%s success_count=%d queue_depth=%d err=%v",
					instanceID,
					snapshot.DrainReason,
					probeType,
					successCount,
					queueDepth,
					err,
				)
				successCount = 0
				continue
			}

			successCount++
			log.CtxDebug(ctx,
				"routeability recovery probe succeeded: instance_id=%s drain_reason=%s probe_type=%s success_count=%d threshold=%d",
				instanceID,
				snapshot.DrainReason,
				probeType,
				successCount,
				successThreshold,
			)
			if successCount < successThreshold {
				continue
			}
			successCount = 0

			if err := gate.MarkHealthy(); err != nil {
				continue
			}
			log.CtxInfo(ctx,
				"routeability recovery triggered: instance_id=%s drain_reason=%s probe_type=%s",
				instanceID,
				snapshot.DrainReason,
				probeType,
			)
			publishCtx, cancelPublish := context.WithTimeout(ctx, runtimeFaultSyncTimeout)
			syncLifecycleState(publishCtx, syncer)
			cancelPublish()
		}
	}()
}

func shouldAttemptRouteabilityRecovery(snapshot cluster.LifecycleSnapshot) bool {
	switch snapshot.Phase {
	case cluster.LifecyclePhaseStopped, cluster.LifecyclePhaseSendClosed:
		return false
	}
	if snapshot.Ready || snapshot.Routeable {
		return false
	}
	switch snapshot.DrainReason {
	case cluster.DrainReasonRouteReadFault, cluster.DrainReasonRouteMirrorOverload, cluster.DrainReasonStatePublishFault, cluster.DrainReasonReconcileFault:
		return true
	default:
		return false
	}
}

func probeRouteabilityRecovery(
	ctx context.Context,
	syncer cluster.InstanceStatePublisher,
	snapshot cluster.LifecycleSnapshot,
	instanceID string,
	snapshotter routeSnapshotter,
	reconciler routeReconciler,
	readProber routeReadProber,
	writeProber routeMirrorWriteProber,
) error {
	switch snapshot.DrainReason {
	case cluster.DrainReasonRouteMirrorOverload:
		if writeProber != nil {
			if err := writeProber.ProbeRouteMirrorWrite(ctx); err != nil {
				return err
			}
		}
	case cluster.DrainReasonStatePublishFault:
		// Probe state publish by attempting a sync
		if syncer != nil {
			if err := syncer.SyncNow(ctx); err != nil {
				return err
			}
		}
	case cluster.DrainReasonReconcileFault:
		// Probe reconcile by attempting reconciliation
		if reconciler != nil && snapshotter != nil {
			if err := reconciler.ReconcileInstanceRoutes(ctx, instanceID, snapshotter.SnapshotRouteConns(instanceID)); err != nil {
				return err
			}
		}
	default:
		// DrainReasonRouteReadFault and others
		if readProber != nil {
			if err := readProber.ProbeRouteRead(ctx, instanceID); err != nil {
				return err
			}
		}
	}
	// Always reconcile and sync after successful probe (except for StatePublishFault/ReconcileFault which already did their specific operation)
	if snapshot.DrainReason != cluster.DrainReasonStatePublishFault && snapshot.DrainReason != cluster.DrainReasonReconcileFault {
		if reconciler != nil && snapshotter != nil {
			if err := reconciler.ReconcileInstanceRoutes(ctx, instanceID, snapshotter.SnapshotRouteConns(instanceID)); err != nil {
				return err
			}
		}
	}
	if snapshot.DrainReason != cluster.DrainReasonStatePublishFault {
		if syncer != nil {
			if err := syncer.SyncNow(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func routeabilityProbeType(reason cluster.DrainReason) string {
	switch reason {
	case cluster.DrainReasonRouteMirrorOverload:
		return "route_mirror_write"
	case cluster.DrainReasonStatePublishFault:
		return "state_publish"
	case cluster.DrainReasonReconcileFault:
		return "reconcile"
	default:
		return "route_read"
	}
}

// startPushSubscription waits for the Redis subscription to become ready before
// startup can proceed. Runtime failures after readiness are handled by the
// lifecycle drain path instead of the startup error path.
func startPushSubscription(
	ctx context.Context,
	pushBus cluster.PushBus,
	instanceId string,
	handler func(context.Context, *cluster.PushEnvelope),
	gate cluster.LifecycleGate,
	syncer cluster.InstanceStatePublisher,
	stats *cluster.RuntimeStats,
) error {
	if pushBus == nil {
		return nil
	}

	readyCh := make(chan struct{})
	errCh := make(chan error, 1)
	var ready atomic.Bool

	go func() {
		err := pushBus.SubscribeInstance(ctx, instanceId, handler, func() {
			if ready.CompareAndSwap(false, true) {
				log.CtxInfo(ctx, "push subscription ready: instance_id=%s", instanceId)
				close(readyCh)
			}
		}, nil)
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		if ready.Load() {
			log.CtxError(ctx, "push subscribe stopped: %v", err)
			handlePushSubscriptionExit(ctx, err, gate, syncer, stats)
			return
		}
		select {
		case errCh <- err:
		default:
		}
	}()

	select {
	case <-readyCh:
		return nil
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitForRouteableDrain(
	ctx context.Context,
	connCounter onlineConnCounter,
	grace time.Duration,
	now func() time.Time,
	sleep func(time.Duration),
) {
	if connCounter == nil || grace <= 0 {
		return
	}

	graceDeadline := now().Add(grace)
	for connCounter.GetOnlineConnCount() > 0 && now().Before(graceDeadline) {
		select {
		case <-ctx.Done():
			return
		default:
		}
		sleep(100 * time.Millisecond)
	}
}

func syncLifecycleState(ctx context.Context, syncer cluster.InstanceStatePublisher) {
	if syncer == nil {
		return
	}
	if err := syncer.SyncNow(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		log.CtxError(ctx, "sync lifecycle state failed: %v", err)
	}
}

func verifyCrossInstanceStartup(
	ctx context.Context,
	instanceId string,
	gate cluster.LifecycleGate,
	snapshotter routeSnapshotter,
	heartbeat cluster.InstanceStatePublisher,
	reconciler routeReconciler,
) error {
	if reconciler != nil && snapshotter != nil {
		if err := reconciler.ReconcileInstanceRoutes(ctx, instanceId, snapshotter.SnapshotRouteConns(instanceId)); err != nil {
			return err
		}
	}
	if heartbeat != nil {
		if err := heartbeat.WriteState(ctx, lifecycleStateForStartup(instanceId, gate)); err != nil {
			return err
		}
	}
	return nil
}

func lifecycleStateForStartup(instanceId string, gate cluster.LifecycleGate) cluster.InstanceState {
	if gate == nil {
		return cluster.InstanceState{
			InstanceId: instanceId,
			Alive:      boolPtr(true),
			Ready:      true,
			Routeable:  true,
		}
	}

	snapshot := gate.Snapshot()
	return cluster.InstanceState{
		InstanceId: instanceId,
		Alive:      boolPtr(true),
		Ready:      snapshot.Ready,
		Routeable:  snapshot.Routeable,
		Draining:   snapshot.Phase == cluster.LifecyclePhaseDraining,
	}
}

func boolPtr(v bool) *bool {
	return &v
}
