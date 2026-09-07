package gateway

import "sync/atomic"

// sendBudget is the node-wide outbound byte budget (ws_send_bytes_total) plus the per-connection
// failure counters Stats reports. Every byte that enters queued leaves it exactly once: through
// write, through a failed enqueue, or in one piece when the connection closes.
type sendBudget struct {
	limit         int64 // 0 = unlimited
	queued        atomic.Int64
	slowConsumers atomic.Int64
	rateLimited   atomic.Int64
	dropped       atomic.Int64
}

func newSendBudget(limit int64) *sendBudget { return &sendBudget{limit: limit} }

// take charges n bytes unconditionally: replies, errors, Kick and Resync never wait for headroom.
func (b *sendBudget) take(n int64) { b.queued.Add(n) }

// tryTake charges n bytes only while the node stays under its cap; a refusal counts as dropped.
func (b *sendBudget) tryTake(n int64) bool {
	if total := b.queued.Add(n); b.limit > 0 && total > b.limit {
		b.queued.Add(-n)
		b.dropped.Add(1)
		return false
	}
	return true
}

func (b *sendBudget) release(n int64) { b.queued.Add(-n) }
