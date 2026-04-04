package cluster

import "context"

// InstanceState describes a websocket instance heartbeat snapshot.
type InstanceState struct {
	InstanceId string `json:"instance_id"`
	Alive      *bool  `json:"alive,omitempty"`
	Ready      bool   `json:"ready"`
	Routeable  bool   `json:"routeable"`
	Draining   bool   `json:"draining"`
	UpdatedAt  int64  `json:"updated_at"`
}

// IsAlive treats a missing Alive field as true for backward compatibility with
// older instance-state snapshots that only implied liveness by key existence.
func (s InstanceState) IsAlive() bool {
	return s.Alive == nil || *s.Alive
}

// RouteConn identifies a single routed websocket connection.
type RouteConn struct {
	UserId     string `json:"user_id"`
	ConnId     string `json:"conn_id"`
	InstanceId string `json:"instance_id"`
	PlatformId int    `json:"platform_id"`
}

// ConnRef is the narrow connection reference used for delivery.
type ConnRef struct {
	UserId     string `json:"user_id"`
	ConnId     string `json:"conn_id"`
	PlatformId int    `json:"platform_id"`
}

// InstanceStateReader reads the current routing snapshot for an instance.
type InstanceStateReader interface {
	ReadState(ctx context.Context, instanceId string) (*InstanceState, error)
}

// InstanceStatePublisher writes and refreshes instance state.
type InstanceStatePublisher interface {
	WriteState(ctx context.Context, state InstanceState) error
	Run(ctx context.Context, instanceId string, snapshot func() InstanceState) error
	SyncNow(ctx context.Context) error
}
