package cluster

import (
	"context"

	"github.com/mbeoliero/nexo/internal/service"
)

type PresenceRouteReader interface {
	GetUsersPresenceConnRefs(ctx context.Context, userIds []string) (map[string][]RouteConn, error)
}

type RouteStorePresenceReader struct {
	store PresenceRouteReader
}

func NewRouteStorePresenceReader(store PresenceRouteReader) *RouteStorePresenceReader {
	return &RouteStorePresenceReader{store: store}
}

func (r *RouteStorePresenceReader) GetUsersPresenceConnRefs(ctx context.Context, userIds []string) (map[string][]service.PresenceConnRef, error) {
	result := make(map[string][]service.PresenceConnRef, len(userIds))
	if r == nil || r.store == nil {
		return result, nil
	}

	routeRefs, err := r.store.GetUsersPresenceConnRefs(ctx, userIds)
	if err != nil {
		return nil, err
	}

	for userId, refs := range routeRefs {
		for _, ref := range refs {
			result[userId] = append(result[userId], service.PresenceConnRef{
				UserId:     ref.UserId,
				ConnId:     ref.ConnId,
				InstanceId: ref.InstanceId,
				PlatformId: ref.PlatformId,
			})
		}
	}
	return result, nil
}
