package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/mbeoliero/kit/log"
	"github.com/redis/go-redis/v9"

	"github.com/mbeoliero/nexo/internal/cluster"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/entity"
	"github.com/mbeoliero/nexo/internal/middleware"
	"github.com/mbeoliero/nexo/internal/service"
	"github.com/mbeoliero/nexo/pkg/constant"
	"github.com/mbeoliero/nexo/pkg/errcode"
)

// WsServer is the WebSocket server
type WsServer struct {
	upgrader       *websocket.Upgrader
	cfg            *config.Config
	userMap        *UserMap
	registerChan   chan *Client
	unregisterChan chan *Client
	pushChan       chan *PushTask
	msgService     *service.MessageService
	convService    *service.ConversationService
	onlineUserNum  atomic.Int64
	onlineConnNum  atomic.Int64
	maxConnNum     int64
	instanceId     string
	runtimeStats   *cluster.RuntimeStats

	pushDelegate          service.MessagePusher
	lifecycleGate         cluster.LifecycleGate
	routeStore            routeMirrorStore
	routeMirrorOnce       sync.Once
	routeMirrorChan       chan routeMirrorTask
	routeMirrorOverflowed atomic.Bool
	routeMirrorMu         sync.Mutex
	onRouteMirrorOverflow func()
}

var clientRegisterEnqueueTimeout = 250 * time.Millisecond

type routeMirrorTask struct {
	ctx      context.Context
	conn     cluster.RouteConn
	register bool
	probe    bool
	done     chan error
}

type routeMirrorStore interface {
	RegisterConn(ctx context.Context, conn cluster.RouteConn) error
	UnregisterConn(ctx context.Context, conn cluster.RouteConn) error
}

// PushTask represents a message push task
type PushTask struct {
	Msg       *entity.Message
	TargetIds []string
	ExcludeId string // Exclude specific connection Id
}

// NewWsServer creates a new WebSocket server
func NewWsServer(cfg *config.Config, rdb *redis.Client, msgService *service.MessageService, convService *service.ConversationService) *WsServer {
	upgrader := &websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			if origin == "" {
				return true
			}
			allowed := cfg.Server.AllowedOrigins
			if len(allowed) == 0 {
				return false
			}
			for _, o := range allowed {
				if o == "*" || strings.EqualFold(o, origin) {
					return true
				}
			}
			return false
		},
	}

	server := &WsServer{
		upgrader:       upgrader,
		cfg:            cfg,
		userMap:        NewUserMap(rdb),
		registerChan:   make(chan *Client, 1000),
		unregisterChan: make(chan *Client, 1000),
		pushChan:       make(chan *PushTask, cfg.WebSocket.PushChannelSize),
		msgService:     msgService,
		convService:    convService,
		maxConnNum:     cfg.WebSocket.MaxConnNum,

		routeMirrorChan: make(chan routeMirrorTask, 1024),
	}

	return server
}

// Run starts the WebSocket server
func (s *WsServer) Run(ctx context.Context) {
	// Start event loop
	go s.eventLoop(ctx)
	// Start push workers
	workerNum := s.cfg.WebSocket.PushWorkerNum
	if workerNum <= 0 {
		workerNum = 10
	}
	for i := 0; i < workerNum; i++ {
		go s.pushLoop(ctx)
	}
	log.Info("started %d push workers", workerNum)
}

// eventLoop handles client registration and unregistration
func (s *WsServer) eventLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case client := <-s.registerChan:
			s.registerClient(ctx, client)
		case client := <-s.unregisterChan:
			s.unregisterClient(ctx, client)
		}
	}
}

// pushLoop handles async message pushing
func (s *WsServer) pushLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-s.pushChan:
			s.processPushTask(ctx, task)
		}
	}
}

// processPushTask processes a single push task
func (s *WsServer) processPushTask(ctx context.Context, task *PushTask) {
	msgData := s.messageToMsgData(task.Msg)

	for _, userId := range task.TargetIds {
		clients, ok := s.userMap.GetAll(userId)
		if !ok {
			continue
		}

		for _, client := range clients {
			// Skip excluded connection
			if task.ExcludeId != "" && client.ConnId == task.ExcludeId {
				continue
			}

			if err := client.PushMessage(ctx, msgData); err != nil {
				log.CtxDebug(ctx, "push to client failed: user_id=%s, conn_id=%s, error=%v", userId, client.ConnId, err)
			}
		}
	}
}

// registerClient registers a client
func (s *WsServer) registerClient(ctx context.Context, client *Client) {
	s.registerClientLocal(ctx, client)
	s.enqueueRouteMirror(ctx, client, true)
}

func (s *WsServer) registerClientLocal(ctx context.Context, client *Client) {
	existingClients, exists := s.userMap.GetAll(client.UserId)
	if !exists {
		s.onlineUserNum.Add(1)
	}

	s.userMap.Register(ctx, client)
	s.onlineConnNum.Add(1)

	log.CtxDebug(ctx, "client registered: user_id=%s, platform_id=%d, conn_id=%s, existing_conns=%d, online_users=%d, online_conns=%d",
		client.UserId, client.PlatformId, client.ConnId, len(existingClients), s.onlineUserNum.Load(), s.onlineConnNum.Load())
}

// unregisterClient unregisters a client
func (s *WsServer) unregisterClient(ctx context.Context, client *Client) {
	s.enqueueRouteMirror(ctx, client, false)
	s.unregisterClientLocal(ctx, client)
}

func (s *WsServer) unregisterClientLocal(ctx context.Context, client *Client) {
	removed, isUserOffline := s.userMap.Unregister(ctx, client)
	if !removed {
		log.CtxDebug(ctx, "client unregister ignored: user_id=%s, platform_id=%d, conn_id=%s", client.UserId, client.PlatformId, client.ConnId)
		return
	}
	s.onlineConnNum.Add(-1)

	if isUserOffline {
		s.onlineUserNum.Add(-1)
	}

	log.CtxDebug(ctx, "client unregistered: user_id=%s, platform_id=%d, conn_id=%s, user_offline=%v, online_users=%d, online_conns=%d",
		client.UserId, client.PlatformId, client.ConnId, isUserOffline, s.onlineUserNum.Load(), s.onlineConnNum.Load())
}

// UnregisterClient queues client for unregistration
func (s *WsServer) UnregisterClient(client *Client) {
	select {
	case s.unregisterChan <- client:
	default:
		log.Warn("unregister channel full: user_id=%s", client.UserId)
		s.unregisterClientNow(context.Background(), client)
	}
}

func (s *WsServer) unregisterClientNow(ctx context.Context, client *Client) {
	s.unregisterClient(ctx, client)
}

// HandleConnection handles a new WebSocket connection (Hertz handler)
func (s *WsServer) HandleConnection(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	if s.lifecycleGate != nil && !s.lifecycleGate.CanAcceptIngress() {
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
		return
	}

	// Check connection limit
	if s.onlineConnNum.Load() >= s.maxConnNum {
		http.Error(w, "connection limit exceeded", http.StatusServiceUnavailable)
		return
	}

	// Parse query parameters
	token := r.URL.Query().Get(QueryToken)
	sendId := r.URL.Query().Get(QuerySendId)
	platformIdStr := r.URL.Query().Get(QueryPlatformId)
	sdkType := r.URL.Query().Get(QuerySDKType)

	if token == "" || sendId == "" {
		http.Error(w, "missing required parameters", http.StatusBadRequest)
		return
	}

	// Validate token (supports external token fallback)
	claims, err := middleware.ParseTokenWithFallback(ctx, token, s.cfg)
	if err != nil {
		log.CtxDebug(ctx, "token validation failed: send_id=%s, error=%v", sendId, err)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	platformId, err := resolvePlatformId(claims.PlatformId, platformIdStr, s.cfg.WebSocket.AllowPlatformIDOverride)
	if err != nil {
		log.CtxWarn(ctx, "websocket invalid platform override: user_id=%s send_id=%s platform_id=%s err=%v", claims.UserId, sendId, platformIdStr, err)
		http.Error(w, "invalid platform_id", http.StatusBadRequest)
		return
	}
	claims.PlatformId = platformId

	// Upgrade connection
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.CtxWarn(ctx, "websocket upgrade failed: %v", err)
		return
	}

	// Create client
	connId := uuid.New().String()
	wsConn := NewWebSocketClientConn(
		conn,
		s.cfg.WebSocket.MaxMessageSize,
		s.cfg.WebSocket.WriteWait,
		s.cfg.WebSocket.PongWait,
		s.cfg.WebSocket.PingPeriod,
		s.cfg.WebSocket.WriteChannelSize,
	)
	client := NewClient(wsConn, claims.UserId, claims.PlatformId, sdkType, token, connId, s)

	// Register client
	if !s.registerClientOrClose(ctx, client) {
		return
	}

	// Start client
	client.Start()
}

func (s *WsServer) registerClientOrClose(ctx context.Context, client *Client) bool {
	if client == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if s.routeStore != nil && s.instanceId != "" {
		s.registerClientLocal(ctx, client)
		if err := s.registerInitialRoute(ctx, client); err != nil {
			if s.runtimeStats != nil {
				s.runtimeStats.IncRouteMirrorErrors()
			}
			s.unregisterClientLocal(ctx, client)
			log.CtxWarn(ctx, "initial route registration failed: instance_id=%s user_id=%s conn_id=%s err=%v", s.instanceId, client.UserId, client.ConnId, err)
			_ = client.Close()
			return false
		}
		return true
	}
	timer := time.NewTimer(clientRegisterEnqueueTimeout)
	defer timer.Stop()

	select {
	case s.registerChan <- client:
		return true
	case <-ctx.Done():
		log.CtxWarn(ctx, "register channel enqueue canceled: user_id=%s conn_id=%s queue_depth=%d err=%v", client.UserId, client.ConnId, len(s.registerChan), ctx.Err())
		_ = client.Close()
		return false
	case <-timer.C:
		log.Warn("register channel blocked, closing upgraded client: user_id=%s conn_id=%s queue_depth=%d", client.UserId, client.ConnId, len(s.registerChan))
		_ = client.Close()
		return false
	}
}

func (s *WsServer) registerInitialRoute(ctx context.Context, client *Client) error {
	if s.routeStore == nil || s.instanceId == "" || client == nil {
		return nil
	}
	return s.routeStore.RegisterConn(ctx, s.routeConnFromClient(client))
}

// AsyncPushToUsers queues a message push to users
func (s *WsServer) AsyncPushToUsers(msg *entity.Message, userIds []string, excludeConnId string) {
	if s.lifecycleGate != nil {
		snapshot := s.lifecycleGate.Snapshot()
		if snapshot.Phase == cluster.LifecyclePhaseSendClosed || snapshot.Phase == cluster.LifecyclePhaseStopped {
			return
		}
	}
	if s.pushDelegate != nil {
		s.pushDelegate.AsyncPushToUsers(msg, userIds, excludeConnId)
		return
	}

	task := &PushTask{
		Msg:       msg,
		TargetIds: userIds,
		ExcludeId: excludeConnId,
	}

	select {
	case s.pushChan <- task:
		// Successfully queued
	default:
		// Queue full, log warning
		log.Warn("push channel full, message dropped: conversation_id=%s, seq=%d", msg.ConversationId, msg.Seq)
	}
}

// SetPushDelegate configures the message push coordinator.
// Must be called before Run().
func (s *WsServer) SetPushDelegate(pusher service.MessagePusher) {
	s.pushDelegate = pusher
}

// SetLifecycleGate configures the lifecycle state machine for graceful shutdown.
// Must be called before Run().
func (s *WsServer) SetLifecycleGate(gate cluster.LifecycleGate) {
	s.lifecycleGate = gate
}

// SetRouteStore configures cross-instance route mirroring.
// Must be called before Run().
func (s *WsServer) SetRouteStore(instanceID string, store routeMirrorStore) {
	s.instanceId = instanceID
	s.routeStore = store
}

// SetRuntimeStats configures runtime metrics collection.
// Must be called before Run().
func (s *WsServer) SetRuntimeStats(stats *cluster.RuntimeStats) {
	s.runtimeStats = stats
}

// SetRouteMirrorOverflowHandler configures the overflow callback.
// Must be called before Run().
func (s *WsServer) SetRouteMirrorOverflowHandler(handler func()) {
	s.routeMirrorMu.Lock()
	defer s.routeMirrorMu.Unlock()
	s.onRouteMirrorOverflow = handler
}

func (s *WsServer) RouteMirrorQueueDepth() int {
	if s == nil || s.routeMirrorChan == nil {
		return 0
	}
	return len(s.routeMirrorChan)
}

func (s *WsServer) ProbeRouteMirrorWrite(ctx context.Context) error {
	if s == nil || s.routeStore == nil || s.instanceId == "" {
		return errors.New("route mirror write probe not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.ensureRouteMirrorLoop(ctx)
	probeConn := cluster.RouteConn{
		UserId:     fmt.Sprintf("__route_mirror_probe__:%s", s.instanceId),
		ConnId:     fmt.Sprintf("probe-%d", time.Now().UnixNano()),
		InstanceId: s.instanceId,
	}
	log.CtxDebug(ctx, "route mirror write probe start: instance_id=%s probe_conn_id=%s queue_depth=%d", s.instanceId, probeConn.ConnId, len(s.routeMirrorChan))
	task := routeMirrorTask{
		ctx:      ctx,
		conn:     probeConn,
		register: true,
		probe:    true,
		done:     make(chan error, 1),
	}

	select {
	case s.routeMirrorChan <- task:
	case <-ctx.Done():
		log.CtxWarn(ctx, "route mirror write probe canceled before enqueue: instance_id=%s probe_conn_id=%s err=%v", s.instanceId, probeConn.ConnId, ctx.Err())
		return ctx.Err()
	default:
		log.CtxWarn(ctx, "route mirror write probe enqueue failed: instance_id=%s probe_conn_id=%s queue_depth=%d", s.instanceId, probeConn.ConnId, len(s.routeMirrorChan))
		return errors.New("route mirror write probe queue full")
	}

	select {
	case err := <-task.done:
		if err != nil {
			log.CtxWarn(ctx, "route mirror write probe failed: instance_id=%s probe_conn_id=%s err=%v", s.instanceId, probeConn.ConnId, err)
			return err
		}
		log.CtxDebug(ctx, "route mirror write probe succeeded: instance_id=%s probe_conn_id=%s", s.instanceId, probeConn.ConnId)
		return nil
	case <-ctx.Done():
		log.CtxWarn(ctx, "route mirror write probe timed out waiting for worker: instance_id=%s probe_conn_id=%s err=%v", s.instanceId, probeConn.ConnId, ctx.Err())
		return ctx.Err()
	}
}

func (s *WsServer) ensureRouteMirrorLoop(ctx context.Context) {
	if s.routeStore == nil || s.instanceId == "" {
		return
	}
	s.routeMirrorOnce.Do(func() {
		if s.routeMirrorChan == nil {
			s.routeMirrorChan = make(chan routeMirrorTask, 1024)
		}
		if ctx == nil {
			ctx = context.Background()
		}
		go s.routeMirrorLoop(ctx)
	})
}

func (s *WsServer) routeMirrorLoop(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-s.routeMirrorChan:
			s.processRouteMirrorTask(task)
		}
	}
}

func (s *WsServer) enqueueRouteMirror(ctx context.Context, client *Client, register bool) {
	if s.routeStore == nil || s.instanceId == "" || client == nil {
		return
	}
	s.ensureRouteMirrorLoop(ctx)
	task := routeMirrorTask{
		ctx:      ctx,
		conn:     s.routeConnFromClient(client),
		register: register,
	}

	select {
	case s.routeMirrorChan <- task:
		s.routeMirrorOverflowed.Store(false)
	default:
		if s.runtimeStats != nil {
			s.runtimeStats.IncRouteMirrorErrors()
		}
		action := "register"
		if !register {
			action = "unregister"
		}
		firstOverflow := s.routeMirrorOverflowed.CompareAndSwap(false, true)
		if firstOverflow {
			log.CtxWarn(ctx, "route mirror queue full: action=%s instance_id=%s user_id=%s conn_id=%s queue_depth=%d", action, s.instanceId, client.UserId, client.ConnId, len(s.routeMirrorChan))
			s.routeMirrorMu.Lock()
			handler := s.onRouteMirrorOverflow
			s.routeMirrorMu.Unlock()
			if handler != nil {
				go handler()
			}
		} else {
			log.CtxDebug(ctx, "route mirror queue still full: action=%s instance_id=%s user_id=%s conn_id=%s queue_depth=%d", action, s.instanceId, client.UserId, client.ConnId, len(s.routeMirrorChan))
		}
	}
}

func (s *WsServer) processRouteMirrorTask(task routeMirrorTask) {
	if s.routeStore == nil || s.instanceId == "" {
		if task.done != nil {
			select {
			case task.done <- errors.New("route mirror not configured"):
			default:
			}
		}
		return
	}
	ctx := task.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	var err error
	action := "register"
	if task.probe {
		action = "probe"
		if err = s.routeStore.RegisterConn(ctx, task.conn); err == nil {
			err = s.routeStore.UnregisterConn(ctx, task.conn)
		}
	} else if task.register {
		err = s.routeStore.RegisterConn(ctx, task.conn)
	} else {
		action = "unregister"
		err = s.routeStore.UnregisterConn(ctx, task.conn)
	}
	if err != nil {
		if s.runtimeStats != nil {
			s.runtimeStats.IncRouteMirrorErrors()
		}
		log.CtxWarn(ctx, "%s route mirror failed: instance_id=%s user_id=%s conn_id=%s err=%v", action, task.conn.InstanceId, task.conn.UserId, task.conn.ConnId, err)
	} else if !task.probe {
		log.CtxDebug(ctx, "%s route mirror succeeded: instance_id=%s user_id=%s conn_id=%s", action, task.conn.InstanceId, task.conn.UserId, task.conn.ConnId)
	}
	if task.done != nil {
		select {
		case task.done <- err:
		default:
		}
	}
}

// GetOnlineUserCount returns online user count
func (s *WsServer) GetOnlineUserCount() int64 {
	return s.onlineUserNum.Load()
}

// GetOnlineConnCount returns online connection count
func (s *WsServer) GetOnlineConnCount() int64 {
	return s.onlineConnNum.Load()
}

// OnlineStatusResult represents a user's online status
type OnlineStatusResult struct {
	UserId               string                  `json:"user_id"`
	Status               int                     `json:"status"` // 0=offline, 1=online
	DetailPlatformStatus []*PlatformStatusDetail `json:"detail_platform_status,omitempty"`
}

// PlatformStatusDetail represents online status detail for a specific platform
type PlatformStatusDetail struct {
	PlatformId   int    `json:"platform_id"`
	PlatformName string `json:"platform_name"`
	ConnId       string `json:"conn_id"`
}

// GetUsersOnlineStatus returns online status for the given user IDs
func (s *WsServer) GetUsersOnlineStatus(userIds []string) []*OnlineStatusResult {
	results := make([]*OnlineStatusResult, 0, len(userIds))
	for _, userId := range userIds {
		result := &OnlineStatusResult{
			UserId: userId,
			Status: constant.StatusOffline,
		}

		clients, ok := s.userMap.GetAll(userId)
		if ok && len(clients) > 0 {
			result.Status = constant.StatusOnline
			result.DetailPlatformStatus = make([]*PlatformStatusDetail, 0, len(clients))
			for _, client := range clients {
				result.DetailPlatformStatus = append(result.DetailPlatformStatus, &PlatformStatusDetail{
					PlatformId:   client.PlatformId,
					PlatformName: constant.PlatformIdToName(client.PlatformId),
					ConnId:       client.ConnId,
				})
			}
		}

		results = append(results, result)
	}
	return results
}

func (s *WsServer) LocalRouteSnapshot(instanceID string) []cluster.RouteConn {
	return s.userMap.SnapshotRouteConns(instanceID)
}

func (s *WsServer) SnapshotRouteConns(instanceID string) []cluster.RouteConn {
	return s.LocalRouteSnapshot(instanceID)
}

func (s *WsServer) GetLocalClient(connID string) (*Client, bool) {
	return s.userMap.GetByConnId(connID)
}

func (s *WsServer) LocalConnRefs(userIDs []string, excludeConnID string) map[string][]cluster.ConnRef {
	result := make(map[string][]cluster.ConnRef, len(userIDs))
	for _, userID := range userIDs {
		clients, ok := s.userMap.GetAll(userID)
		if !ok {
			continue
		}
		for _, client := range clients {
			if excludeConnID != "" && client.ConnId == excludeConnID {
				continue
			}
			result[userID] = append(result[userID], cluster.ConnRef{
				UserId:     client.UserId,
				ConnId:     client.ConnId,
				PlatformId: client.PlatformId,
			})
		}
	}
	return result
}

// DeliverLocal best-effort pushes an already-prepared payload to local websocket
// clients. It re-checks shutdown state between refs so force-close can stop an
// in-flight delivery loop promptly.
func (s *WsServer) DeliverLocal(ctx context.Context, refs []cluster.ConnRef, payload *cluster.PushPayload) error {
	if payload == nil || payload.Message == nil {
		return nil
	}
	if s.shouldStopLocalDelivery(ctx) {
		return nil
	}

	msgData := s.pushMessageToMsgData(payload.Message)
	for _, ref := range refs {
		if s.shouldStopLocalDelivery(ctx) {
			return nil
		}
		client, ok := s.GetLocalClient(ref.ConnId)
		if !ok {
			continue
		}
		if err := client.PushMessage(ctx, msgData); err != nil {
			log.CtxDebug(ctx, "deliver local failed: conn_id=%s err=%v", ref.ConnId, err)
		}
	}
	return nil
}

// shouldStopLocalDelivery reports whether local realtime delivery should stop
// because the request context was canceled or the send path has been closed.
func (s *WsServer) shouldStopLocalDelivery(ctx context.Context) bool {
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	if s.lifecycleGate != nil {
		snapshot := s.lifecycleGate.Snapshot()
		return snapshot.Phase == cluster.LifecyclePhaseSendClosed || snapshot.Phase == cluster.LifecyclePhaseStopped
	}
	return false
}

func (s *WsServer) GetUsersLocalPresence(userIDs []string) map[string][]*service.PlatformStatusDetail {
	result := make(map[string][]*service.PlatformStatusDetail, len(userIDs))
	for _, userID := range userIDs {
		clients, ok := s.userMap.GetAll(userID)
		if !ok {
			continue
		}
		for _, client := range clients {
			result[userID] = append(result[userID], &service.PlatformStatusDetail{
				PlatformId:   client.PlatformId,
				PlatformName: constant.PlatformIdToName(client.PlatformId),
				ConnId:       client.ConnId,
			})
		}
	}
	return result
}

// messageToMsgData converts entity.Message to MessageData
func (s *WsServer) messageToMsgData(msg *entity.Message) *MessageData {
	custom := ""
	if msg.ContentCustom != nil {
		custom = *msg.ContentCustom
	}
	return &MessageData{
		ServerMsgId:    msg.Id,
		ConversationId: msg.ConversationId,
		Seq:            msg.Seq,
		ClientMsgId:    msg.ClientMsgId,
		SenderId:       msg.SenderId,
		RecvId:         msg.RecvId,
		GroupId:        msg.GroupId,
		SessionType:    msg.SessionType,
		MsgType:        msg.MsgType,
		Content: struct {
			Text   string `json:"text,omitempty"`
			Image  string `json:"image,omitempty"`
			Video  string `json:"video,omitempty"`
			Audio  string `json:"audio,omitempty"`
			File   string `json:"file,omitempty"`
			Custom string `json:"custom,omitempty"`
		}{
			Text:   msg.ContentText,
			Image:  msg.ContentImage,
			Video:  msg.ContentVideo,
			Audio:  msg.ContentAudio,
			File:   msg.ContentFile,
			Custom: custom,
		},
		SendAt: msg.SendAt,
	}
}

func (s *WsServer) pushMessageToMsgData(msg *cluster.PushMessage) *MessageData {
	if msg == nil {
		return nil
	}
	return &MessageData{
		ServerMsgId:    msg.ServerMsgId,
		ConversationId: msg.ConversationId,
		Seq:            msg.Seq,
		ClientMsgId:    msg.ClientMsgId,
		SenderId:       msg.SenderId,
		RecvId:         msg.RecvId,
		GroupId:        msg.GroupId,
		SessionType:    msg.SessionType,
		MsgType:        msg.MsgType,
		Content: struct {
			Text   string `json:"text,omitempty"`
			Image  string `json:"image,omitempty"`
			Video  string `json:"video,omitempty"`
			Audio  string `json:"audio,omitempty"`
			File   string `json:"file,omitempty"`
			Custom string `json:"custom,omitempty"`
		}{
			Text:   msg.Content.Text,
			Image:  msg.Content.Image,
			Video:  msg.Content.Video,
			Audio:  msg.Content.Audio,
			File:   msg.Content.File,
			Custom: msg.Content.Custom,
		},
		SendAt: msg.SendAt,
	}
}

func (s *WsServer) routeConnFromClient(client *Client) cluster.RouteConn {
	return cluster.RouteConn{
		UserId:     client.UserId,
		ConnId:     client.ConnId,
		InstanceId: s.instanceId,
		PlatformId: client.PlatformId,
	}
}

func resolvePlatformId(claimPlatformId int, override string, allowOverride bool) (int, error) {
	if override == "" {
		return claimPlatformId, nil
	}
	pid, err := strconv.Atoi(override)
	if err != nil {
		return 0, err
	}
	if !isKnownPlatformId(pid) {
		return 0, fmt.Errorf("unsupported platform_id=%d", pid)
	}
	if !allowOverride && pid != claimPlatformId {
		return 0, fmt.Errorf("platform_id override disabled")
	}
	return pid, nil
}

func isKnownPlatformId(platformId int) bool {
	switch platformId {
	case constant.PlatformIdIOS,
		constant.PlatformIdAndroid,
		constant.PlatformIdWindows,
		constant.PlatformIdMacOS,
		constant.PlatformIdWeb:
		return true
	default:
		return false
	}
}

// ========== Message Handlers ==========

// HandleGetNewestSeq handles get newest seq request
func (s *WsServer) HandleGetNewestSeq(ctx context.Context, client *Client, req *WSRequest) ([]byte, error) {
	var getSeqReq GetNewestSeqReq
	if err := json.Unmarshal(req.Data, &getSeqReq); err != nil {
		return nil, errcode.ErrInvalidParam
	}

	seqs := make(map[string]int64)
	for _, convId := range getSeqReq.ConversationIds {
		maxSeq, _ := s.msgService.GetMaxSeq(ctx, client.UserId, convId)
		seqs[convId] = maxSeq
	}

	resp := GetNewestSeqResp{Seqs: seqs}
	return json.Marshal(resp)
}

// HandleSendMsg handles send message request
func (s *WsServer) HandleSendMsg(ctx context.Context, client *Client, req *WSRequest) ([]byte, error) {
	var sendReq SendMsgReq
	if err := json.Unmarshal(req.Data, &sendReq); err != nil {
		return nil, errcode.ErrInvalidParam
	}

	// Build service request
	svcReq := &service.SendMessageRequest{
		ClientMsgId: sendReq.ClientMsgId,
		RecvId:      sendReq.RecvId,
		GroupId:     sendReq.GroupId,
		SessionType: sendReq.SessionType,
		MsgType:     sendReq.MsgType,
		Content: entity.MessageContent{
			Text:   sendReq.Content.Text,
			Image:  sendReq.Content.Image,
			Video:  sendReq.Content.Video,
			Audio:  sendReq.Content.Audio,
			File:   sendReq.Content.File,
			Custom: sendReq.Content.Custom,
		},
	}

	if s.lifecycleGate != nil {
		release, gateErr := s.lifecycleGate.AcquireSendLease()
		if gateErr != nil {
			return nil, gateErr
		}
		defer release()
	}
	msg, err := s.msgService.SendMessage(ctx, client.UserId, svcReq)
	if err != nil {
		return nil, err
	}

	resp := SendMsgResp{
		ServerMsgId:    msg.Id,
		ConversationId: msg.ConversationId,
		Seq:            msg.Seq,
		ClientMsgId:    msg.ClientMsgId,
		SendAt:         msg.SendAt,
	}

	return json.Marshal(resp)
}

// HandlePullMsg handles pull messages request
func (s *WsServer) HandlePullMsg(ctx context.Context, client *Client, req *WSRequest) ([]byte, error) {
	var pullReq PullMsgReq
	if err := json.Unmarshal(req.Data, &pullReq); err != nil {
		return nil, errcode.ErrInvalidParam
	}

	svcReq := &service.PullMessagesRequest{
		ConversationId: pullReq.ConversationId,
		BeginSeq:       pullReq.BeginSeq,
		EndSeq:         pullReq.EndSeq,
		Limit:          pullReq.Limit,
	}

	messages, maxSeq, err := s.msgService.PullMessages(ctx, client.UserId, svcReq)
	if err != nil {
		return nil, err
	}

	msgDataList := make([]*MessageData, 0, len(messages))
	for _, msg := range messages {
		msgDataList = append(msgDataList, s.messageToMsgData(msg))
	}

	resp := PullMsgResp{
		Messages: msgDataList,
		MaxSeq:   maxSeq,
	}

	return json.Marshal(resp)
}

// HandlePullMsgBySeqList handles pull messages by seq list request
func (s *WsServer) HandlePullMsgBySeqList(ctx context.Context, client *Client, req *WSRequest) ([]byte, error) {
	// For now, use the same handler as PullMsg
	// In a full implementation, you'd pass the seq list to the service
	return s.HandlePullMsg(ctx, client, req)
}

// HandleGetConvMaxReadSeq handles get conversation max/read seq request
func (s *WsServer) HandleGetConvMaxReadSeq(ctx context.Context, client *Client, req *WSRequest) ([]byte, error) {
	var getSeqReq GetConvMaxReadSeqReq
	if err := json.Unmarshal(req.Data, &getSeqReq); err != nil {
		return nil, errcode.ErrInvalidParam
	}

	maxSeq, readSeq, err := s.convService.GetMaxReadSeq(ctx, client.UserId, getSeqReq.ConversationId)
	if err != nil {
		return nil, err
	}

	unreadCount := maxSeq - readSeq
	if unreadCount < 0 {
		unreadCount = 0
	}

	resp := GetConvMaxReadSeqResp{
		MaxSeq:      maxSeq,
		ReadSeq:     readSeq,
		UnreadCount: unreadCount,
	}

	return json.Marshal(resp)
}
