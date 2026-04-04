package cluster

import (
	"context"
	"time"
)

type LocalRouteSnapshotter interface {
	SnapshotRouteConns(instanceId string) []RouteConn
}

type InstanceRouteReconciler interface {
	ReconcileInstanceRoutes(ctx context.Context, instanceId string, want []RouteConn) error
}

func RunRouteReconcileLoop(
	ctx context.Context,
	interval time.Duration,
	instanceId string,
	snapshotter LocalRouteSnapshotter,
	routeStore InstanceRouteReconciler,
	stats *RuntimeStats,
) error {
	if snapshotter == nil || routeStore == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	if interval <= 0 {
		interval = time.Second
	}

	reconcile := func() error {
		startedAt := time.Now()
		err := routeStore.ReconcileInstanceRoutes(ctx, instanceId, snapshotter.SnapshotRouteConns(instanceId))
		if stats != nil {
			stats.RecordReconcileDuration(time.Since(startedAt))
		}
		return err
	}
	if err := reconcile(); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := reconcile(); err != nil {
				return err
			}
		}
	}
}
