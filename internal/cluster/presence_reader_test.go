package cluster

import (
	"context"
	"testing"
)

type stubPresenceRouteReader struct {
	refs map[string][]RouteConn
	err  error
}

func (s *stubPresenceRouteReader) GetUsersPresenceConnRefs(_ context.Context, userIDs []string) (map[string][]RouteConn, error) {
	if s.err != nil {
		return nil, s.err
	}
	result := make(map[string][]RouteConn, len(userIDs))
	for _, userID := range userIDs {
		result[userID] = append([]RouteConn(nil), s.refs[userID]...)
	}
	return result, nil
}

func TestRouteStorePresenceReaderAcceptsPresenceRouteReaderInterface(t *testing.T) {
	reader := NewRouteStorePresenceReader(&stubPresenceRouteReader{
		refs: map[string][]RouteConn{
			"u1": {{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}},
		},
	})

	got, err := reader.GetUsersPresenceConnRefs(context.Background(), []string{"u1"})
	if err != nil {
		t.Fatalf("get users presence conn refs: %v", err)
	}
	if len(got["u1"]) != 1 {
		t.Fatalf("presence conn ref count mismatch: %+v", got["u1"])
	}
	if got["u1"][0].UserId != "u1" || got["u1"][0].ConnId != "c1" || got["u1"][0].InstanceId != "i1" || got["u1"][0].PlatformId != 5 {
		t.Fatalf("presence conn ref mismatch: %+v", got["u1"][0])
	}
}
