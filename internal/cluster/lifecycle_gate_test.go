package cluster

import (
	"context"
	"testing"
	"time"
)

func TestAcquireSendLeaseAllowedOnlyInServing(t *testing.T) {
	gate := NewLifecycleGate()

	release, err := gate.AcquireSendLease()
	if err != nil {
		t.Fatalf("acquire send lease in serving: %v", err)
	}
	release()

	if err := gate.BeginDrain(DrainReasonPlanned); err != nil {
		t.Fatalf("begin drain: %v", err)
	}

	if _, err := gate.AcquireSendLease(); err == nil {
		t.Fatal("expected acquire send lease to fail after drain begins")
	}
}

func TestBeginPlannedDrainRejectsNewLeaseButKeepsRouteable(t *testing.T) {
	gate := NewLifecycleGate()

	if err := gate.BeginDrain(DrainReasonPlanned); err != nil {
		t.Fatalf("begin planned drain: %v", err)
	}

	snapshot := gate.Snapshot()
	if snapshot.Phase != LifecyclePhaseDraining {
		t.Fatalf("phase mismatch: got %s", snapshot.Phase)
	}
	if snapshot.Ready {
		t.Fatal("ready should be false")
	}
	if !snapshot.Routeable {
		t.Fatal("routeable should remain true during planned drain")
	}
	if !snapshot.IngressClosed {
		t.Fatal("ingress should be closed during planned drain")
	}
}

func TestBeginSubscribeFaultDrainMarksUnrouteableImmediately(t *testing.T) {
	gate := NewLifecycleGate()

	if err := gate.BeginDrain(DrainReasonSubscribeFault); err != nil {
		t.Fatalf("begin subscribe fault drain: %v", err)
	}

	snapshot := gate.Snapshot()
	if snapshot.Phase != LifecyclePhaseDraining {
		t.Fatalf("phase mismatch: got %s", snapshot.Phase)
	}
	if snapshot.Routeable {
		t.Fatal("routeable should be false for subscribe fault drain")
	}
}

func TestBeginStatePublishFaultDrainMarksUnrouteableImmediately(t *testing.T) {
	gate := NewLifecycleGate()

	if err := gate.MarkDegraded(DrainReasonStatePublishFault); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}

	snapshot := gate.Snapshot()
	if snapshot.Phase != LifecyclePhaseDraining {
		t.Fatalf("phase mismatch: got %s want %s", snapshot.Phase, LifecyclePhaseDraining)
	}
	if snapshot.Ready {
		t.Fatal("ready should be false for state publish fault degradation")
	}
	if snapshot.Routeable {
		t.Fatal("routeable should be false for state publish fault drain")
	}
	if gate.CanAcceptIngress() {
		t.Fatal("ingress should close for state publish fault degradation")
	}
	if _, err := gate.AcquireSendLease(); err == nil {
		t.Fatal("expected send lease rejection after state publish fault degradation")
	}
}

func TestBeginReconcileFaultDrainMarksUnrouteableImmediately(t *testing.T) {
	gate := NewLifecycleGate()

	if err := gate.MarkDegraded(DrainReasonReconcileFault); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}

	snapshot := gate.Snapshot()
	if snapshot.Phase != LifecyclePhaseDraining {
		t.Fatalf("phase mismatch: got %s want %s", snapshot.Phase, LifecyclePhaseDraining)
	}
	if snapshot.Ready {
		t.Fatal("ready should be false for reconcile fault degradation")
	}
	if snapshot.Routeable {
		t.Fatal("routeable should be false for reconcile fault drain")
	}
	if gate.CanAcceptIngress() {
		t.Fatal("ingress should close for reconcile fault degradation")
	}
	if _, err := gate.AcquireSendLease(); err == nil {
		t.Fatal("expected send lease rejection after reconcile fault degradation")
	}
}

func TestMarkHealthyRestoresServingReadinessAndRouteability(t *testing.T) {
	gate := NewLifecycleGate()

	if err := gate.MarkDegraded(DrainReasonRouteReadFault); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}
	if err := gate.MarkHealthy(); err != nil {
		t.Fatalf("mark healthy: %v", err)
	}

	snapshot := gate.Snapshot()
	if snapshot.Phase != LifecyclePhaseServing {
		t.Fatalf("phase mismatch: got %s", snapshot.Phase)
	}
	if !snapshot.Ready {
		t.Fatal("ready should be restored")
	}
	if !snapshot.Routeable {
		t.Fatal("routeable should be restored")
	}
	if snapshot.IngressClosed {
		t.Fatal("ingress should remain open after recovery")
	}
	if snapshot.DrainReason != "" {
		t.Fatalf("drain reason should be cleared, got %s", snapshot.DrainReason)
	}
}

func TestMarkHealthyDoesNotExitPlannedDrain(t *testing.T) {
	gate := NewLifecycleGate()

	if err := gate.BeginDrain(DrainReasonPlanned); err != nil {
		t.Fatalf("begin drain: %v", err)
	}
	if err := gate.MarkHealthy(); err != nil {
		t.Fatalf("mark healthy: %v", err)
	}

	snapshot := gate.Snapshot()
	if snapshot.Phase != LifecyclePhaseDraining {
		t.Fatalf("phase mismatch: got %s", snapshot.Phase)
	}
	if snapshot.Ready {
		t.Fatal("ready should remain false during planned drain")
	}
	if !snapshot.Routeable {
		t.Fatal("planned drain should remain routeable")
	}
	if !snapshot.IngressClosed {
		t.Fatal("ingress should remain closed during planned drain")
	}
	if snapshot.DrainReason != DrainReasonPlanned {
		t.Fatalf("drain reason mismatch: got %s", snapshot.DrainReason)
	}
}

func TestCloseSendPathWaitsForInflightToDrain(t *testing.T) {
	gate := NewLifecycleGate()

	release, err := gate.AcquireSendLease()
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}

	done := make(chan struct{})
	go func() {
		if err := gate.CloseSendPath(); err != nil {
			t.Errorf("close send path: %v", err)
		}
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("close send path returned before inflight lease released")
	case <-time.After(50 * time.Millisecond):
	}

	release()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close send path did not finish after release")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := gate.WaitInflightZero(ctx); err != nil {
		t.Fatalf("wait inflight zero: %v", err)
	}
	if got := gate.Snapshot().Phase; got != LifecyclePhaseSendClosed {
		t.Fatalf("phase mismatch: got %s", got)
	}
}

func TestForceCloseSendPathDoesNotWaitForInflight(t *testing.T) {
	gate := NewLifecycleGate()

	_, err := gate.AcquireSendLease()
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}

	if err := gate.ForceCloseSendPath(); err != nil {
		t.Fatalf("force close send path: %v", err)
	}

	snapshot := gate.Snapshot()
	if snapshot.Phase != LifecyclePhaseSendClosed {
		t.Fatalf("phase mismatch: got %s", snapshot.Phase)
	}
	if snapshot.Routeable {
		t.Fatal("routeable should be false after force close")
	}
}
