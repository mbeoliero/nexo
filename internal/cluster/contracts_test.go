package cluster

import (
	"encoding/json"
	"testing"
)

func TestRouteConnJSONRoundTrip(t *testing.T) {
	want := RouteConn{
		UserId:     "u1",
		ConnId:     "c1",
		InstanceId: "i1",
		PlatformId: 5,
	}

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal route conn: %v", err)
	}

	var got RouteConn
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal route conn: %v", err)
	}

	if got != want {
		t.Fatalf("route conn mismatch: got %+v want %+v", got, want)
	}
}

func TestInstanceStateJSONRoundTrip(t *testing.T) {
	want := InstanceState{
		InstanceId: "i1",
		Ready:      true,
		Routeable:  false,
		Draining:   true,
		UpdatedAt:  123,
	}

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal instance state: %v", err)
	}

	var got InstanceState
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal instance state: %v", err)
	}

	if got != want {
		t.Fatalf("instance state mismatch: got %+v want %+v", got, want)
	}
}

func TestInstanceStateMissingAliveDefaultsToAlive(t *testing.T) {
	var got InstanceState
	if err := json.Unmarshal([]byte(`{"instance_id":"i1","ready":false,"routeable":false,"draining":false,"updated_at":123}`), &got); err != nil {
		t.Fatalf("unmarshal instance state: %v", err)
	}

	if !got.IsAlive() {
		t.Fatalf("expected missing alive to default to true: %+v", got)
	}
}

func TestInstanceStateJSONRoundTripPreservesExplicitAliveFalse(t *testing.T) {
	alive := false
	want := InstanceState{
		InstanceId: "i1",
		Alive:      &alive,
		Ready:      false,
		Routeable:  false,
		Draining:   false,
		UpdatedAt:  456,
	}

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal instance state: %v", err)
	}

	var got InstanceState
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal instance state: %v", err)
	}

	if got.Alive == nil || *got.Alive {
		t.Fatalf("expected explicit alive=false to round-trip: %+v", got)
	}
}
