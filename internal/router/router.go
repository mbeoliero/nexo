package router

import (
	"context"
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/adaptor"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/mbeoliero/nexo/internal/cluster"
	"github.com/mbeoliero/nexo/internal/gateway"
	"github.com/mbeoliero/nexo/internal/handler"
	"github.com/mbeoliero/nexo/internal/middleware"
)

// SetupRouter sets up all routes
func SetupRouter(
	h *server.Hertz,
	handlers *Handlers,
	wsServer *gateway.WsServer,
	gate cluster.LifecycleGate,
	crossInstanceEnabled bool,
	readyDetailEnabled bool,
	stats *cluster.RuntimeStats,
) {
	// CORS middleware
	h.Use(middleware.CORS())

	// Health check
	h.GET("/health", func(ctx context.Context, c *app.RequestContext) {
		c.JSON(consts.StatusOK, map[string]string{"status": "ok"})
	})
	h.GET("/ready", readyHandler(gate, crossInstanceEnabled, readyDetailEnabled, stats))

	// Auth routes (no auth required)
	authGroup := h.Group("/auth")
	{
		authGroup.POST("/register", handlers.Auth.Register)
		authGroup.POST("/login", handlers.Auth.Login)
	}

	// User routes (auth required)
	userGroup := h.Group("/user", middleware.JWTAuth())
	{
		userGroup.GET("/info", handlers.User.GetUserInfo)
		userGroup.GET("/profile/:user_id", handlers.User.GetUserInfoById)
		userGroup.PUT("/update", handlers.User.UpdateUserInfo)
		userGroup.POST("/batch_info", handlers.User.GetUsersInfo)
		userGroup.POST("/get_users_online_status", handlers.User.GetUsersOnlineStatus)
	}

	// Group routes (auth required)
	groupGroup := h.Group("/group", middleware.JWTAuth())
	{
		groupGroup.POST("/create", handlers.Group.CreateGroup)
		groupGroup.POST("/join", handlers.Group.JoinGroup)
		groupGroup.POST("/quit", handlers.Group.QuitGroup)
		groupGroup.GET("/info", handlers.Group.GetGroupInfo)
		groupGroup.GET("/members", handlers.Group.GetGroupMembers)
	}

	// Message routes (auth required)
	msgGroup := h.Group("/msg", middleware.JWTAuth())
	{
		msgGroup.POST("/send", handlers.Message.SendMessage)
		msgGroup.GET("/pull", handlers.Message.PullMessages)
		msgGroup.GET("/max_seq", handlers.Message.GetMaxSeq)
	}

	// Conversation routes (auth required)
	convGroup := h.Group("/conversation", middleware.JWTAuth())
	{
		convGroup.GET("/list", handlers.Conversation.GetConversationList)
		convGroup.GET("/info", handlers.Conversation.GetConversation)
		convGroup.PUT("/update", handlers.Conversation.UpdateConversation)
		convGroup.POST("/mark_read", handlers.Conversation.MarkRead)
		convGroup.GET("/max_read_seq", handlers.Conversation.GetMaxReadSeq)
		convGroup.GET("/unread_count", handlers.Conversation.GetUnreadCount)
	}

	// WebSocket route using net/http handler via Hertz adaptor
	h.GET("/ws", adaptor.HertzHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsServer.HandleConnection(r.Context(), w, r)
	})))
}

// Handlers holds all HTTP handlers
type Handlers struct {
	Auth         *handler.AuthHandler
	User         *handler.UserHandler
	Group        *handler.GroupHandler
	Message      *handler.MessageHandler
	Conversation *handler.ConversationHandler
}

func readyHandler(gate cluster.LifecycleGate, crossInstanceEnabled bool, readyDetailEnabled bool, stats *cluster.RuntimeStats) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		ready := true
		var snapshot cluster.LifecycleSnapshot
		if gate != nil {
			snapshot = gate.Snapshot()
			ready = snapshot.Ready
		}

		body := map[string]any{
			"status": "ready",
		}
		if !ready {
			body["status"] = "not_ready"
		}

		if crossInstanceEnabled && readyDetailEnabled {
			clusterBody := map[string]any{}
			if stats != nil {
				statSnapshot := stats.Snapshot()
				clusterBody["dispatch_queue_depth"] = statSnapshot.DispatchQueueDepth
				clusterBody["dispatch_dropped_total"] = statSnapshot.DispatchDroppedTotal
				clusterBody["route_read_errors_total"] = statSnapshot.RouteReadErrorsTotal
				clusterBody["route_mirror_errors_total"] = statSnapshot.RouteMirrorErrorsTotal
				clusterBody["subscription_fault_total"] = statSnapshot.SubscriptionFaultTotal
				clusterBody["state_publish_fault_total"] = statSnapshot.StatePublishFaultTotal
				clusterBody["reconcile_fault_total"] = statSnapshot.ReconcileFaultTotal
				clusterBody["last_subscription_fault_at"] = statSnapshot.LastSubscriptionFaultAt
				clusterBody["reconcile_duration_ms"] = statSnapshot.ReconcileDurationMs
				clusterBody["presence_partial_total"] = statSnapshot.PresencePartialTotal
			}
			if snapshot.DrainReason != "" {
				clusterBody["drain_reason"] = snapshot.DrainReason
			}
			body["cluster"] = clusterBody
		}

		if !ready {
			c.JSON(consts.StatusServiceUnavailable, body)
			return
		}
		c.JSON(consts.StatusOK, body)
	}
}
