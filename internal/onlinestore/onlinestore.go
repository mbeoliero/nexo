package onlinestore

import "context"

type ConnRef struct {
	UserId     string
	PlatformId int
	ConnId     string
}

// OnlineStore is the global connection registry (design §6.5). It feeds offline-push
// decisions and /user/online_status only; routing and kicks never consult it.
type OnlineStore interface {
	Add(ctx context.Context, nodeId string, c ConnRef) error
	// Remove takes the same nodeId Add did: without it a driver keyed by (platform, node, conn)
	// has to scan the user's whole connection set to find the member to delete.
	Remove(ctx context.Context, nodeId string, c ConnRef) error
	// Renew extends every listed connection of this node; the gateway calls it on a timer. revived
	// holds the refs this call had to create rather than extend, which is the only part of a renew
	// that changed anyone's presence: the gateway announces exactly those, so a driver that cannot
	// restore a lost registration returns none rather than the whole list (design §7.4).
	Renew(ctx context.Context, nodeId string, conns []ConnRef) (revived []ConnRef, err error)
	// Online maps user id → platforms with a live connection; absent = offline.
	Online(ctx context.Context, userIds []string) (map[string][]int, error)
	// PurgeNode drops what this node left behind before it last died.
	PurgeNode(ctx context.Context, nodeId string) error
}
