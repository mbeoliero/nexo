package message

import (
	"maps"
	"slices"
	"sync"
	"time"
)

const memberCacheMax = 10000

// memberCache is a per-node TTL cache of group rosters. It only narrows push
// candidates; VisibleTo authorizes, so a stale entry can never leak a message.
type memberCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]memberEntry
	now     func() time.Time
}

type memberEntry struct {
	ids     []string
	expires time.Time
}

func newMemberCache(ttl time.Duration) *memberCache {
	return &memberCache{ttl: ttl, entries: map[string]memberEntry{}, now: time.Now}
}

func (c *memberCache) get(groupId string) ([]string, bool) {
	if c.ttl <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[groupId]
	if !ok || c.now().After(e.expires) {
		delete(c.entries, groupId)
		return nil, false
	}
	return slices.Clone(e.ids), true // callers filter in place
}

func (c *memberCache) set(groupId string, ids []string) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= memberCacheMax {
		now := c.now()
		maps.DeleteFunc(c.entries, func(_ string, e memberEntry) bool { return now.After(e.expires) })
		if len(c.entries) >= memberCacheMax {
			clear(c.entries) // still full of live entries: drop everything rather than grow
		}
	}
	c.entries[groupId] = memberEntry{ids: slices.Clone(ids), expires: c.now().Add(c.ttl)}
}

func (c *memberCache) invalidate(groupId string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, groupId)
}

// InvalidateGroup drops the cached roster; the gateway calls it on group_changed.
func (s *Service) InvalidateGroup(groupId string) { s.members.Load().invalidate(groupId) }

// SetMemberCacheTtl sets the roster cache TTL (limits.group_member_cache_ttl); zero disables it.
func (s *Service) SetMemberCacheTtl(ttl time.Duration) { s.members.Store(newMemberCache(ttl)) }
