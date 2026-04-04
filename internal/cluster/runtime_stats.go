package cluster

import (
	"sync/atomic"
	"time"
)

// RuntimeStatsSnapshot is the compact operator-facing view returned by /ready.
type RuntimeStatsSnapshot struct {
	DispatchQueueDepth      int64 `json:"dispatch_queue_depth"`
	DispatchDroppedTotal    int64 `json:"dispatch_dropped_total"`
	RouteReadErrorsTotal    int64 `json:"route_read_errors_total"`
	RouteMirrorErrorsTotal  int64 `json:"route_mirror_errors_total"`
	RemotePublishFaultTotal int64 `json:"remote_publish_fault_total"`
	SubscriptionFaultTotal  int64 `json:"subscription_fault_total"`
	StatePublishFaultTotal  int64 `json:"state_publish_fault_total"`
	ReconcileFaultTotal     int64 `json:"reconcile_fault_total"`
	LastSubscriptionFaultAt int64 `json:"last_subscription_fault_at"`
	ReconcileDurationMs     int64 `json:"reconcile_duration_ms"`
	PresencePartialTotal    int64 `json:"presence_partial_total"`
}

// RuntimeStats keeps lightweight in-memory counters for cross-instance health.
type RuntimeStats struct {
	dispatchQueueDepth      atomic.Int64
	dispatchDroppedTotal    atomic.Int64
	routeReadErrorsTotal    atomic.Int64
	routeMirrorErrorsTotal  atomic.Int64
	remotePublishFaultTotal atomic.Int64
	subscriptionFaultTotal  atomic.Int64
	statePublishFaultTotal  atomic.Int64
	reconcileFaultTotal     atomic.Int64
	lastSubscriptionFaultAt atomic.Int64
	reconcileDurationMs     atomic.Int64
	presencePartialTotal    atomic.Int64
}

// NewRuntimeStats creates an empty stats collector.
func NewRuntimeStats() *RuntimeStats {
	return &RuntimeStats{}
}

// Snapshot returns a copy suitable for logs, readiness, and diagnostics.
func (s *RuntimeStats) Snapshot() RuntimeStatsSnapshot {
	if s == nil {
		return RuntimeStatsSnapshot{}
	}
	return RuntimeStatsSnapshot{
		DispatchQueueDepth:      s.dispatchQueueDepth.Load(),
		DispatchDroppedTotal:    s.dispatchDroppedTotal.Load(),
		RouteReadErrorsTotal:    s.routeReadErrorsTotal.Load(),
		RouteMirrorErrorsTotal:  s.routeMirrorErrorsTotal.Load(),
		RemotePublishFaultTotal: s.remotePublishFaultTotal.Load(),
		SubscriptionFaultTotal:  s.subscriptionFaultTotal.Load(),
		StatePublishFaultTotal:  s.statePublishFaultTotal.Load(),
		ReconcileFaultTotal:     s.reconcileFaultTotal.Load(),
		LastSubscriptionFaultAt: s.lastSubscriptionFaultAt.Load(),
		ReconcileDurationMs:     s.reconcileDurationMs.Load(),
		PresencePartialTotal:    s.presencePartialTotal.Load(),
	}
}

// SetDispatchQueueDepth records the current local async dispatch backlog.
func (s *RuntimeStats) SetDispatchQueueDepth(depth int) {
	if s == nil {
		return
	}
	s.dispatchQueueDepth.Store(int64(depth))
}

// IncDispatchDropped records local realtime fanout tasks dropped after queue
// saturation.
func (s *RuntimeStats) IncDispatchDropped() {
	if s == nil {
		return
	}
	s.dispatchDroppedTotal.Add(1)
}

// IncRouteReadErrors records route-store read failures that forced local-only
// degradation.
func (s *RuntimeStats) IncRouteReadErrors() {
	if s == nil {
		return
	}
	s.routeReadErrorsTotal.Add(1)
}

// IncRouteMirrorErrors records failed immediate route mirror writes from the local runtime.
func (s *RuntimeStats) IncRouteMirrorErrors() {
	if s == nil {
		return
	}
	s.routeMirrorErrorsTotal.Add(1)
}

// RecordRemotePublishFault marks an exhausted cross-instance publish failure.
func (s *RuntimeStats) RecordRemotePublishFault() {
	if s == nil {
		return
	}
	s.remotePublishFaultTotal.Add(1)
}

// RecordSubscriptionFault marks a cross-instance subscription failure.
func (s *RuntimeStats) RecordSubscriptionFault(at time.Time) {
	if s == nil {
		return
	}
	s.subscriptionFaultTotal.Add(1)
	if !at.IsZero() {
		s.lastSubscriptionFaultAt.Store(at.UnixMilli())
	}
}

// RecordStatePublishFault marks an instance-state publication/runtime failure.
func (s *RuntimeStats) RecordStatePublishFault() {
	if s == nil {
		return
	}
	s.statePublishFaultTotal.Add(1)
}

// RecordReconcileFault marks a route reconcile runtime failure.
func (s *RuntimeStats) RecordReconcileFault() {
	if s == nil {
		return
	}
	s.reconcileFaultTotal.Add(1)
}

// RecordReconcileDuration stores the latest route reconcile duration.
func (s *RuntimeStats) RecordReconcileDuration(d time.Duration) {
	if s == nil {
		return
	}
	s.reconcileDurationMs.Store(d.Milliseconds())
}

// IncPresencePartial records degraded presence reads that only used local data.
func (s *RuntimeStats) IncPresencePartial() {
	if s == nil {
		return
	}
	s.presencePartialTotal.Add(1)
}
