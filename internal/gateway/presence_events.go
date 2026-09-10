package gateway

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"slices"

	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/bus"
	"github.com/mbeoliero/nexo/internal/identity"
)

const (
	// The longest id identity.Valid admits is a native one: nx__ plus a canonical 36-byte UUID.
	// Actor ids are shorter (the prefix plus at most 19 int64 digits). Both are pure ASCII with
	// no byte JSON has to escape, so an id encodes to exactly len(id)+2 and costs that plus its
	// separating comma inside the array.
	maxUserIdJsonBytes = identity.PrefixLength + 36 + len(`"",`)
	// presenceChunkSize is a constant instead of a count derived from the envelope so a long
	// node_id cannot silently shrink chunks toward one id per bus event; publishOnlineChanged
	// fails loudly against presenceEnvelopeBudget instead.
	presenceChunkSize = 100
	// presenceChunkJsonBytes is a full chunk on the wire: the ids plus the object they are wrapped
	// in. The wrapper is not free and is not covered by any per-id count, so leaving it out of the
	// budget would pass a node_id whose event then overruns MaxPayloadBytes and fails per chunk.
	presenceChunkJsonBytes = presenceChunkSize*maxUserIdJsonBytes + len(`{"user_ids":[]}`)
	// presenceEnvelopeBudget is what a full chunk leaves for the envelope around it.
	presenceEnvelopeBudget = bus.MaxPayloadBytes - presenceChunkJsonBytes
)

// Caller releases the presence lock and keeps its bounded operation context alive through publication.
func (g *Gateway) publishOnlineChanged(ctx context.Context, userIds []string) {
	if len(userIds) == 0 {
		return
	}
	userIds = slices.Clone(userIds)
	slices.Sort(userIds)
	userIds = slices.Compact(userIds)
	g.markOnlineChanged(userIds)
	if g.deps.Bus == nil {
		return
	}

	fail := func(err error) {
		g.onlinePublishFails.Add(1)
		log.CtxError(ctx, "bus publish presence_changed: %v", errcode.ErrBusFailed.Wrap(err))
	}
	ev := bus.Event{Type: bus.TypePresenceChanged, NodeId: g.cfg.NodeId}
	empty, err := json.Marshal(bus.PresenceChanged{UserIds: []string{}})
	if err != nil {
		fail(err)
		return
	}
	ev.Payload = empty
	envelope, err := json.Marshal(ev)
	if err != nil {
		fail(err)
		return
	}
	// baseSize is the envelope with no ids in it; the payload is spliced in verbatim, so a chunk
	// costs baseSize plus its own encoded length.
	baseSize := len(envelope) - len(empty)
	if baseSize > presenceEnvelopeBudget {
		fail(fmt.Errorf("node_id (%d bytes) leaves %d of the %d bytes a %d-id presence chunk needs",
			len(g.cfg.NodeId), bus.MaxPayloadBytes-baseSize, presenceChunkJsonBytes, presenceChunkSize))
		return
	}
	// Each chunk is attempted once and counted once. A failed chunk is not retried and does not
	// abandon the rest: the event carries no authoritative state, so every user it still can reach
	// gets their reread sooner, and the ones it cannot wait for the periodic query (design §7.4).
	for chunk := range slices.Chunk(userIds, presenceChunkSize) {
		payload, err := json.Marshal(bus.PresenceChanged{UserIds: chunk})
		if err == nil && baseSize+len(payload) > bus.MaxPayloadBytes {
			// maxUserIdJsonBytes only bounds ids this project issues; a custom auth.Authenticator
			// can hand the gateway anything, so the encoded chunk is still measured.
			err = fmt.Errorf("presence event with %d user ids exceeds %d bytes", len(chunk), bus.MaxPayloadBytes)
		}
		if err == nil {
			ev.Payload = payload
			err = g.deps.Bus.Publish(ctx, ev)
		}
		if err != nil {
			fail(err)
		}
	}
}
