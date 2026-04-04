package router

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/mbeoliero/nexo/internal/cluster"
)

func TestReadyHandlerReturns503WhenNotReady(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	if err := gate.BeginDrain(cluster.DrainReasonPlanned); err != nil {
		t.Fatalf("begin drain: %v", err)
	}

	ctx := app.NewContext(0)
	readyHandler(gate, false, false, nil)(context.Background(), ctx)

	if got := ctx.Response.StatusCode(); got != 503 {
		t.Fatalf("status code mismatch: got %d want 503", got)
	}
}

func TestReadyHandlerOmitsClusterSummaryWhenDetailsDisabled(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	stats := cluster.NewRuntimeStats()

	ctx := app.NewContext(0)
	readyHandler(gate, true, false, stats)(context.Background(), ctx)

	var body map[string]any
	if err := json.Unmarshal(ctx.Response.Body(), &body); err != nil {
		t.Fatalf("unmarshal ready body: %v", err)
	}
	if _, ok := body["cluster"]; ok {
		t.Fatalf("expected cluster details to be hidden by default: %+v", body)
	}
}

func TestReadyHandlerIncludesClusterSummaryAndDrainReasonWhenEnabled(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	stats := cluster.NewRuntimeStats()
	stats.SetDispatchQueueDepth(3)
	stats.IncDispatchDropped()
	stats.IncRouteMirrorErrors()
	stats.RecordSubscriptionFault(time.UnixMilli(1234))
	stats.RecordStatePublishFault()
	stats.RecordReconcileFault()
	if err := gate.BeginDrain(cluster.DrainReasonSubscribeFault); err != nil {
		t.Fatalf("begin drain: %v", err)
	}

	ctx := app.NewContext(0)
	readyHandler(gate, true, true, stats)(context.Background(), ctx)

	var body map[string]any
	if err := json.Unmarshal(ctx.Response.Body(), &body); err != nil {
		t.Fatalf("unmarshal ready body: %v", err)
	}

	clusterBody, ok := body["cluster"].(map[string]any)
	if !ok {
		t.Fatalf("cluster body missing: %+v", body)
	}
	if got := clusterBody["dispatch_queue_depth"]; got != float64(3) {
		t.Fatalf("dispatch queue depth mismatch: got %v want 3", got)
	}
	if got := clusterBody["route_mirror_errors_total"]; got != float64(1) {
		t.Fatalf("route mirror error count mismatch: got %v want 1", got)
	}
	if got := clusterBody["drain_reason"]; got != string(cluster.DrainReasonSubscribeFault) {
		t.Fatalf("drain reason mismatch: got %v want %s", got, cluster.DrainReasonSubscribeFault)
	}
	if got := clusterBody["subscription_fault_total"]; got != float64(1) {
		t.Fatalf("subscription fault count mismatch: got %v want 1", got)
	}
	if got := clusterBody["state_publish_fault_total"]; got != float64(1) {
		t.Fatalf("state publish fault count mismatch: got %v want 1", got)
	}
	if got := clusterBody["reconcile_fault_total"]; got != float64(1) {
		t.Fatalf("reconcile fault count mismatch: got %v want 1", got)
	}
}
