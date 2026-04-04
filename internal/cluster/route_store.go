package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mbeoliero/kit/log"
	"github.com/redis/go-redis/v9"

	"github.com/mbeoliero/nexo/pkg/constant"
)

var ErrInstanceStateReaderNotConfigured = errors.New("instance state reader not configured")

type RouteStore struct {
	rdb      *redis.Client
	routeTTL time.Duration
	states   InstanceStateReader
}

func NewRouteStore(rdb *redis.Client, routeTTL time.Duration, states InstanceStateReader) *RouteStore {
	return &RouteStore{
		rdb:      rdb,
		routeTTL: routeTTL,
		states:   states,
	}
}

func (s *RouteStore) RegisterConn(ctx context.Context, conn RouteConn) error {
	data, err := json.Marshal(conn)
	if err != nil {
		return err
	}

	pipe := s.rdb.TxPipeline()
	connKey := routeConnKey(conn.InstanceId, conn.ConnId)
	pipe.Set(ctx, connKey, data, s.routeTTL)
	pipe.SAdd(ctx, routeUserKey(conn.UserId), connKey)
	pipe.Expire(ctx, routeUserKey(conn.UserId), s.routeTTL)
	pipe.SAdd(ctx, routeInstanceKey(conn.InstanceId), connKey)
	pipe.Expire(ctx, routeInstanceKey(conn.InstanceId), s.routeTTL)
	_, err = pipe.Exec(ctx)
	return err
}

func (s *RouteStore) UnregisterConn(ctx context.Context, conn RouteConn) error {
	pipe := s.rdb.TxPipeline()
	connKey := routeConnKey(conn.InstanceId, conn.ConnId)
	pipe.Del(ctx, connKey)
	pipe.SRem(ctx, routeUserKey(conn.UserId), connKey)
	pipe.SRem(ctx, routeInstanceKey(conn.InstanceId), connKey)
	_, err := pipe.Exec(ctx)
	return err
}

// GetUsersConnRefs returns connection routes for message delivery.
// Only includes connections on instances that are Routeable (ready to receive messages).
func (s *RouteStore) GetUsersConnRefs(ctx context.Context, userIds []string) (map[string][]RouteConn, error) {
	return s.getUsersConnRefs(ctx, userIds, true)
}

// GetUsersPresenceConnRefs returns connection routes for presence queries.
// Includes connections on any alive instance, even if draining/degraded.
// This shows accurate online status but may include users who can't receive messages.
func (s *RouteStore) GetUsersPresenceConnRefs(ctx context.Context, userIds []string) (map[string][]RouteConn, error) {
	return s.getUsersConnRefs(ctx, userIds, false)
}

func (s *RouteStore) ReconcileInstanceRoutes(ctx context.Context, instanceId string, want []RouteConn) error {
	existing, err := s.rdb.SMembers(ctx, routeInstanceKey(instanceId)).Result()
	if err != nil {
		return err
	}

	wantKeys := make(map[string]RouteConn, len(want))
	for _, conn := range want {
		wantKeys[routeConnKey(conn.InstanceId, conn.ConnId)] = conn
	}
	if err := s.upsertRoutes(ctx, want); err != nil {
		return err
	}

	var staleKeys []string
	for _, routeKey := range existing {
		if _, ok := wantKeys[routeKey]; ok {
			continue
		}
		staleKeys = append(staleKeys, routeKey)
	}
	if len(staleKeys) > 0 {
		if err := s.removeStaleInstanceRoutes(ctx, instanceId, staleKeys); err != nil {
			return err
		}
	}

	return s.rdb.Expire(ctx, routeInstanceKey(instanceId), s.routeTTL).Err()
}

// ProbeRouteRead exercises the current route-read surface used by dispatch
// recovery. It prefers probing a real user route owned by this instance so the
// same GetUsersConnRefs path is exercised; when the instance currently has no
// routes it falls back to validating that instance-state reads still work.
func (s *RouteStore) ProbeRouteRead(ctx context.Context, instanceId string) error {
	routeKeys, err := s.rdb.SMembers(ctx, routeInstanceKey(instanceId)).Result()
	if err != nil {
		log.CtxWarn(ctx, "route read probe failed to list instance routes: instance_id=%s err=%v", instanceId, err)
		return err
	}

	if len(routeKeys) > 0 {
		// Use pipelined GET instead of MGET for cluster compatibility
		pipe := s.rdb.Pipeline()
		getCmds := make([]*redis.StringCmd, len(routeKeys))
		for i, key := range routeKeys {
			getCmds[i] = pipe.Get(ctx, key)
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			log.CtxWarn(ctx, "route read probe failed to load instance routes: instance_id=%s route_key_count=%d err=%v", instanceId, len(routeKeys), err)
			return err
		}
		for idx := range routeKeys {
			val, err := getCmds[idx].Result()
			if errors.Is(err, redis.Nil) {
				continue
			}
			if err != nil {
				log.CtxWarn(ctx, "route read probe failed to load route key: instance_id=%s route_key=%s err=%v", instanceId, routeKeys[idx], err)
				return err
			}
			conn, ok := decodeRouteConn(val)
			if !ok {
				continue
			}
			log.CtxDebug(ctx, "route read probe using user route: instance_id=%s user_id=%s conn_id=%s route_key_count=%d", instanceId, conn.UserId, conn.ConnId, len(routeKeys))
			_, probeErr := s.GetUsersConnRefs(ctx, []string{conn.UserId})
			if probeErr != nil {
				log.CtxWarn(ctx, "route read probe failed via user route: instance_id=%s user_id=%s conn_id=%s err=%v", instanceId, conn.UserId, conn.ConnId, probeErr)
				return probeErr
			}
			log.CtxDebug(ctx, "route read probe succeeded via user route: instance_id=%s user_id=%s conn_id=%s", instanceId, conn.UserId, conn.ConnId)
			return nil
		}
	}

	if s.states == nil {
		return ErrInstanceStateReaderNotConfigured
	}
	log.CtxDebug(ctx, "route read probe using state-reader fallback: instance_id=%s route_key_count=0", instanceId)
	_, err = s.readCachedState(ctx, instanceId, make(map[string]stateCacheEntry))
	if err != nil {
		log.CtxWarn(ctx, "route read probe failed via state-reader fallback: instance_id=%s err=%v", instanceId, err)
		return err
	}
	log.CtxDebug(ctx, "route read probe succeeded via state-reader fallback: instance_id=%s", instanceId)
	return err
}

func (s *RouteStore) getUsersConnRefs(ctx context.Context, userIds []string, requireRouteable bool) (map[string][]RouteConn, error) {
	if len(userIds) == 0 {
		return make(map[string][]RouteConn), nil
	}

	// Phase 1: Pipeline all SMEMBERS calls
	pipe := s.rdb.Pipeline()
	smembersCmds := make(map[string]*redis.StringSliceCmd, len(userIds))
	for _, userId := range userIds {
		smembersCmds[userId] = pipe.SMembers(ctx, routeUserKey(userId))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	// Phase 2: Collect all route keys and pipeline one MGET
	type userRouteKeys struct {
		userId string
		keys   []string
	}
	var allUserKeys []userRouteKeys
	var allRouteKeys []string
	routeKeyToUser := make(map[string]string)

	for _, userId := range userIds {
		keys, err := smembersCmds[userId].Result()
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			continue
		}
		allUserKeys = append(allUserKeys, userRouteKeys{userId: userId, keys: keys})
		for _, key := range keys {
			allRouteKeys = append(allRouteKeys, key)
			routeKeyToUser[key] = userId
		}
	}

	if len(allRouteKeys) == 0 {
		return make(map[string][]RouteConn), nil
	}

	// Phase 2b: Pipeline individual GET commands (cluster-safe)
	// MGET doesn't work in cluster mode when keys are on different hash slots
	getPipe := s.rdb.Pipeline()
	getCmds := make([]*redis.StringCmd, len(allRouteKeys))
	for i, key := range allRouteKeys {
		getCmds[i] = getPipe.Get(ctx, key)
	}
	if _, err := getPipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	// Phase 3: Process results with cached state reads
	result := make(map[string][]RouteConn, len(userIds))
	stateCache := make(map[string]stateCacheEntry)
	danglingByUser := make(map[string][]string)

	for idx, routeKey := range allRouteKeys {
		userId := routeKeyToUser[routeKey]
		val, err := getCmds[idx].Result()
		if errors.Is(err, redis.Nil) {
			danglingByUser[userId] = append(danglingByUser[userId], routeKey)
			continue
		}
		if err != nil {
			return nil, err
		}
		conn, ok := decodeRouteConn(val)
		if !ok {
			danglingByUser[userId] = append(danglingByUser[userId], routeKey)
			continue
		}

		state, err := s.readCachedState(ctx, conn.InstanceId, stateCache)
		if err != nil {
			return nil, err
		}
		if !shouldIncludeState(state, requireRouteable) {
			continue
		}

		result[userId] = append(result[userId], conn)
	}

	// Cleanup dangling keys
	for userId, dangling := range danglingByUser {
		if err := s.cleanupDanglingRouteKeys(ctx, userId, dangling); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func shouldIncludeState(state *InstanceState, requireRouteable bool) bool {
	if state == nil {
		return false
	}
	if requireRouteable {
		return state.Routeable
	}
	return state.IsAlive()
}

type stateCacheEntry struct {
	loaded bool
	state  *InstanceState
}

func (s *RouteStore) readCachedState(ctx context.Context, instanceID string, cache map[string]stateCacheEntry) (*InstanceState, error) {
	if entry, ok := cache[instanceID]; ok && entry.loaded {
		return entry.state, nil
	}
	if s.states == nil {
		return nil, ErrInstanceStateReaderNotConfigured
	}
	state, err := s.states.ReadState(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	cache[instanceID] = stateCacheEntry{
		loaded: true,
		state:  state,
	}
	return state, nil
}

func decodeRouteConn(value any) (RouteConn, bool) {
	payload, ok := redisValueBytes(value)
	if !ok {
		return RouteConn{}, false
	}
	var conn RouteConn
	if err := json.Unmarshal(payload, &conn); err != nil {
		return RouteConn{}, false
	}
	return conn, true
}

func redisValueBytes(value any) ([]byte, bool) {
	switch v := value.(type) {
	case nil:
		return nil, false
	case string:
		return []byte(v), true
	case []byte:
		return append([]byte(nil), v...), true
	default:
		return nil, false
	}
}

func (s *RouteStore) cleanupDanglingRouteKeys(ctx context.Context, userId string, routeKeys []string) error {
	if len(routeKeys) == 0 {
		return nil
	}
	pipe := s.rdb.TxPipeline()
	pipe.SRem(ctx, routeUserKey(userId), anyStrings(routeKeys)...)
	for _, routeKey := range routeKeys {
		pipe.Del(ctx, routeKey)
		if instanceID, _, ok := parseRouteConnKey(routeKey); ok {
			pipe.SRem(ctx, routeInstanceKey(instanceID), routeKey)
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (s *RouteStore) upsertRoutes(ctx context.Context, conns []RouteConn) error {
	if len(conns) == 0 {
		return nil
	}

	pipe := s.rdb.TxPipeline()
	seenUsers := make(map[string]struct{})
	seenInstances := make(map[string]struct{})
	for _, conn := range conns {
		data, err := json.Marshal(conn)
		if err != nil {
			return err
		}

		connKey := routeConnKey(conn.InstanceId, conn.ConnId)
		pipe.Set(ctx, connKey, data, s.routeTTL)
		pipe.SAdd(ctx, routeUserKey(conn.UserId), connKey)
		pipe.SAdd(ctx, routeInstanceKey(conn.InstanceId), connKey)
		seenUsers[conn.UserId] = struct{}{}
		seenInstances[conn.InstanceId] = struct{}{}
	}
	for userID := range seenUsers {
		pipe.Expire(ctx, routeUserKey(userID), s.routeTTL)
	}
	for instanceID := range seenInstances {
		pipe.Expire(ctx, routeInstanceKey(instanceID), s.routeTTL)
	}

	_, err := pipe.Exec(ctx)
	return err
}

func (s *RouteStore) removeStaleInstanceRoutes(ctx context.Context, instanceID string, routeKeys []string) error {
	// Use pipelined GET instead of MGET for cluster compatibility
	getPipe := s.rdb.Pipeline()
	getCmds := make([]*redis.StringCmd, len(routeKeys))
	for i, key := range routeKeys {
		getCmds[i] = getPipe.Get(ctx, key)
	}
	if _, err := getPipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	pipe := s.rdb.TxPipeline()
	for idx, routeKey := range routeKeys {
		pipe.Del(ctx, routeKey)
		pipe.SRem(ctx, routeInstanceKey(instanceID), routeKey)

		val, err := getCmds[idx].Result()
		if err == nil {
			if conn, ok := decodeRouteConn(val); ok {
				pipe.SRem(ctx, routeUserKey(conn.UserId), routeKey)
			}
		}
	}

	_, err := pipe.Exec(ctx)
	return err
}

func anyStrings(values []string) []interface{} {
	if len(values) == 0 {
		return nil
	}
	result := make([]interface{}, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	return result
}

func routeConnKey(instanceId, connId string) string {
	return constant.GetRedisKeyPrefix() + fmt.Sprintf("cluster:route:conn:%s:%s", instanceId, connId)
}

func routeUserKey(userId string) string {
	return constant.GetRedisKeyPrefix() + fmt.Sprintf("cluster:route:user:%s", userId)
}

func routeInstanceKey(instanceId string) string {
	return constant.GetRedisKeyPrefix() + fmt.Sprintf("cluster:route:instance:%s", instanceId)
}

func pushChannelKey(instanceId string) string {
	return constant.GetRedisKeyPrefix() + fmt.Sprintf("cluster:push:%s", instanceId)
}

func parseRouteConnKey(routeKey string) (string, string, bool) {
	raw := strings.TrimPrefix(routeKey, constant.GetRedisKeyPrefix())
	parts := strings.Split(raw, ":")
	if len(parts) < 5 {
		return "", "", false
	}
	return parts[len(parts)-2], parts[len(parts)-1], true
}
