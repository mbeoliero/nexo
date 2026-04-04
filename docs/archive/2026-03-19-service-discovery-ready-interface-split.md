# Service-Discovery-Ready Interface Split Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Decouple instance discovery from route storage so future `etcd`/`consul` style instance registries can replace the current Redis-backed instance-state path without rewriting the current Redis route path or realtime push path.

**Architecture:** Keep `RouteStore` as the Redis-backed high-frequency route index and keep `PushBus` as the current per-instance transport. Do not introduce a broad `RouteRegistry`. Instead, add a provider-neutral instance-state seam, inject that seam into `RouteStore` for `alive/routeable` filtering, and keep downstream consumers depending on narrow interfaces rather than concrete `*RouteStore` or `*InstanceManager` types.

**Tech Stack:** Go, Hertz, Redis, current `internal/cluster`, `internal/gateway`, `internal/service`, `cmd/server`

---

## Scope And Constraints

**In scope**
- Introduce small cluster interfaces for reading and publishing instance state.
- Remove `RouteStore`'s direct dependency on Redis `instance_alive` reads.
- Keep `PushCoordinator`, `WsServer`, and `PresenceService` depending on narrow interfaces.
- Normalize `cmd/server/main.go` to use cluster-level interfaces instead of local ad-hoc interface definitions for instance-state publication and sync.
- Preserve current single-instance behavior and existing route-only multi-instance startup/shutdown invariants.

**Out of scope**
- Replacing Redis route storage.
- Replacing Redis Pub/Sub.
- Introducing a generic `RouteRegistry` abstraction that combines route storage and instance discovery.
- Implementing an actual `etcd` or `consul` backend in this change.

## Desired End State

After this refactor:

- `RouteStore` remains the concrete Redis implementation for route keys and indexes.
- `InstanceManager` remains the concrete Redis implementation for instance-state publication, but now sits behind provider-neutral interfaces.
- `RouteStore` determines `alive/routeable` by consulting an injected `InstanceStateReader`, not by reading Redis keys directly.
- `RouteStore` fails explicitly when route filtering is required but no `InstanceStateReader` is wired, instead of silently degrading to empty results.
- `PushCoordinator` still depends only on `RemoteRouteReader`.
- `WsServer` still depends only on `routeMirrorStore`.
- Presence and reconcile helpers accept narrow interfaces instead of concrete `*RouteStore`.
- The composition root in `cmd/server/main.go` is the only place that decides which concrete instance-state provider is used.

---

### Task 1: Introduce provider-neutral instance-state interfaces

**Files:**
- Modify: `internal/cluster/contracts.go`
- Modify: `internal/cluster/instance_manager.go`
- Test: `internal/cluster/instance_manager_test.go`

**Step 1: Write the failing test**

Add interface-usage coverage to `internal/cluster/instance_manager_test.go`:

```go
func TestInstanceManagerCanBeUsedAsInstanceStateReaderAndPublisher(t *testing.T) {}
func TestInstanceManagerSyncNowStillPublishesUpdatedSnapshot(t *testing.T) {}
```

The first test should assign `NewInstanceManager(...)` to variables typed as the new interfaces and exercise at least one read/write path through those interfaces.

**Step 2: Run the focused test to verify it fails**

Run:

```bash
go test ./internal/cluster -run 'TestInstanceManagerCanBeUsedAsInstanceStateReaderAndPublisher|TestInstanceManagerSyncNowStillPublishesUpdatedSnapshot' -count=1
```

Expected:
- FAIL because the provider-neutral interfaces do not exist yet.

**Step 3: Write minimal implementation**

In `internal/cluster/contracts.go`, add:

```go
type InstanceStateReader interface {
    ReadState(ctx context.Context, instanceID string) (*InstanceState, error)
}

type InstanceStatePublisher interface {
    WriteState(ctx context.Context, state InstanceState) error
    Run(ctx context.Context, instanceID string, snapshot func() InstanceState) error
    SyncNow(ctx context.Context) error
}
```

Keep the existing data contracts in place. Do not fold route-storage behaviors into these interfaces.

In `internal/cluster/instance_manager.go`:
- keep `InstanceManager` concrete
- ensure it continues to implement `ReadState`, `WriteState`, `Run`, and `SyncNow`
- add compile-time assertions if useful:

```go
var _ InstanceStateReader = (*InstanceManager)(nil)
var _ InstanceStatePublisher = (*InstanceManager)(nil)
```

**Step 4: Run the focused test to verify it passes**

Run:

```bash
go test ./internal/cluster -run 'TestInstanceManagerCanBeUsedAsInstanceStateReaderAndPublisher|TestInstanceManagerSyncNowStillPublishesUpdatedSnapshot' -count=1
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add internal/cluster/contracts.go internal/cluster/instance_manager.go internal/cluster/instance_manager_test.go
git commit -m "refactor: add provider-neutral instance state interfaces"
```

---

### Task 2: Make RouteStore consume injected instance-state reads

**Files:**
- Modify: `internal/cluster/route_store.go`
- Modify: `internal/cluster/route_store_test.go`
- Modify: `internal/cluster/route_store_integration_test.go`
- Modify: `internal/cluster/presence_reader_test.go`

**Step 1: Write the failing tests**

Add or update route-store tests so route filtering is driven by an injected state reader instead of an implicit Redis lookup:

```go
func TestGetUsersConnRefsUsesInjectedInstanceStateReader(t *testing.T) {}
func TestGetUsersPresenceConnRefsUsesInjectedAliveStateReader(t *testing.T) {}
func TestGetUsersConnRefsSkipsConnWhenStateReaderReturnsNil(t *testing.T) {}
func TestGetUsersConnRefsReturnsErrorWhenStateReaderNotConfiguredAndRoutePresent(t *testing.T) {}
```

For unit tests, use a stub `InstanceStateReader` instead of constructing a real `InstanceManager`.

Keep one integration path in `internal/cluster/route_store_integration_test.go` that wires a real `InstanceManager` into `RouteStore` and proves the end-to-end Redis behavior still works.

**Step 2: Run the focused tests to verify they fail**

Run:

```bash
go test ./internal/cluster -run 'TestGetUsersConnRefsUsesInjectedInstanceStateReader|TestGetUsersPresenceConnRefsUsesInjectedAliveStateReader|TestGetUsersConnRefsSkipsConnWhenStateReaderReturnsNil|TestGetUsersConnRefsReturnsErrorWhenStateReaderNotConfiguredAndRoutePresent|TestRouteStoreRegisterUnregisterRoundTripWithRedis' -count=1
```

Expected:
- FAIL because `RouteStore` does not accept an injected instance-state reader yet.

**Step 3: Write minimal implementation**

Change `internal/cluster/route_store.go` so `RouteStore` becomes:

```go
type RouteStore struct {
    rdb      *redis.Client
    routeTTL time.Duration
    states   InstanceStateReader
}

func NewRouteStore(rdb *redis.Client, routeTTL time.Duration, states InstanceStateReader) *RouteStore
```

Implementation rules:
- keep route-key, user-index, and instance-index behavior unchanged
- remove the internal `readInstanceState(...)` helper that reads Redis directly
- in `getUsersConnRefs(...)`, consult `s.states.ReadState(ctx, conn.InstanceId)` when `s.states != nil`
- if the state reader returns `nil`, treat the instance as unavailable
- if a valid route requires state filtering but no `InstanceStateReader` is configured, return an explicit misconfiguration error instead of silently degrading to empty results
- push lookup still requires `routeable=true`
- presence lookup still requires only that the instance is alive

Do not change the immediate register/unregister mirror logic or reconcile semantics.

**Step 4: Run the focused tests to verify they pass**

Run:

```bash
go test ./internal/cluster -run 'TestGetUsersConnRefsUsesInjectedInstanceStateReader|TestGetUsersPresenceConnRefsUsesInjectedAliveStateReader|TestGetUsersConnRefsSkipsConnWhenStateReaderReturnsNil|TestGetUsersConnRefsReturnsErrorWhenStateReaderNotConfiguredAndRoutePresent|TestRouteStoreRegisterUnregisterRoundTripWithRedis' -count=1
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add internal/cluster/route_store.go internal/cluster/route_store_test.go internal/cluster/route_store_integration_test.go internal/cluster/presence_reader_test.go
git commit -m "refactor: decouple route store from concrete instance state backend"
```

---

### Task 3: Narrow concrete RouteStore consumers without introducing a broad registry abstraction

**Files:**
- Modify: `internal/cluster/presence_reader.go`
- Modify: `internal/cluster/presence_reader_test.go`
- Modify: `internal/cluster/route_reconcile.go`
- Modify: `internal/cluster/route_reconcile_test.go`

**Step 1: Write the failing tests**

Add compile-time or behavior coverage proving these helpers no longer require a concrete `*RouteStore`:

```go
func TestRouteStorePresenceReaderAcceptsPresenceRouteReaderInterface(t *testing.T) {}
func TestRunRouteReconcileLoopAcceptsRouteReconcilerInterface(t *testing.T) {}
```

Use small stubs instead of constructing a real `RouteStore`.

**Step 2: Run the focused tests to verify they fail**

Run:

```bash
go test ./internal/cluster -run 'TestRouteStorePresenceReaderAcceptsPresenceRouteReaderInterface|TestRunRouteReconcileLoopAcceptsRouteReconcilerInterface' -count=1
```

Expected:
- FAIL because the helpers still require concrete `*RouteStore`.

**Step 3: Write minimal implementation**

In `internal/cluster/presence_reader.go`, introduce a narrow dependency:

```go
type PresenceRouteReader interface {
    GetUsersPresenceConnRefs(ctx context.Context, userIDs []string) (map[string][]RouteConn, error)
}
```

Change `RouteStorePresenceReader` to depend on `PresenceRouteReader`, not `*RouteStore`.

In `internal/cluster/route_reconcile.go`, introduce a narrow reconciler dependency:

```go
type InstanceRouteReconciler interface {
    ReconcileInstanceRoutes(ctx context.Context, instanceID string, want []RouteConn) error
}
```

Change `RunRouteReconcileLoop(...)` to accept `InstanceRouteReconciler` instead of `*RouteStore`.

Do not introduce a combined `RouteRegistry` or any interface that merges write-path mirroring, remote reads, presence reads, and reconcile into one type.

**Step 4: Run the focused tests to verify they pass**

Run:

```bash
go test ./internal/cluster -run 'TestRouteStorePresenceReaderAcceptsPresenceRouteReaderInterface|TestRunRouteReconcileLoopAcceptsRouteReconcilerInterface' -count=1
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add internal/cluster/presence_reader.go internal/cluster/presence_reader_test.go internal/cluster/route_reconcile.go internal/cluster/route_reconcile_test.go
git commit -m "refactor: narrow route store helper dependencies"
```

---

### Task 4: Normalize startup and shutdown wiring around cluster-level instance-state interfaces

**Files:**
- Modify: `cmd/server/main.go`
- Modify: `cmd/server/main_test.go`

**Step 1: Write the failing tests**

Extend `cmd/server/main_test.go` with interface-oriented wiring coverage:

```go
func TestVerifyCrossInstanceStartupAcceptsInstanceStatePublisher(t *testing.T) {}
func TestHandlePushSubscriptionExitSyncsThroughInstanceStatePublisher(t *testing.T) {}
func TestMainHelpersDoNotRequireConcreteInstanceManagerForStateSync(t *testing.T) {}
```

Use stubs typed as the new cluster interfaces rather than local ad-hoc interfaces.

**Step 2: Run the focused tests to verify they fail**

Run:

```bash
go test ./cmd/server -run 'TestVerifyCrossInstanceStartupAcceptsInstanceStatePublisher|TestHandlePushSubscriptionExitSyncsThroughInstanceStatePublisher|TestMainHelpersDoNotRequireConcreteInstanceManagerForStateSync' -count=1
```

Expected:
- FAIL because `cmd/server/main.go` still defines local interface shims for instance-state publication and sync.

**Step 3: Write minimal implementation**

In `cmd/server/main.go`:
- remove local ad-hoc interfaces that model instance-state publication/sync
- replace them with `cluster.InstanceStatePublisher` where publication or sync is needed
- keep the composition root responsible for choosing the concrete implementation
- wire `NewRouteStore(..., stateReader)` with the same concrete `InstanceManager` in current Redis mode

The composition root may still instantiate a concrete `*cluster.InstanceManager`; that is acceptable. The goal is to keep the rest of the startup/shutdown flow dependent on cluster-level interfaces rather than on Redis-flavored helper types.

Do not change startup order or shutdown semantics:
- `subscription ready -> initial reconcile -> advertise alive/routeable`
- `BeginDrain -> routeable grace -> MarkRouteableOff -> wait inflight -> wait dispatch -> close send path`

**Step 4: Run the focused tests to verify they pass**

Run:

```bash
go test ./cmd/server -run 'TestVerifyCrossInstanceStartupAcceptsInstanceStatePublisher|TestHandlePushSubscriptionExitSyncsThroughInstanceStatePublisher|TestMainHelpersDoNotRequireConcreteInstanceManagerForStateSync' -count=1
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add cmd/server/main.go cmd/server/main_test.go
git commit -m "refactor: normalize startup wiring around instance state interfaces"
```

---

### Task 5: Run focused regression verification and architecture audit

**Files:**
- Modify: none
- Test: existing focused suites

**Step 1: Run cluster tests**

Run:

```bash
go test ./internal/cluster -count=1
```

Expected:
- PASS

**Step 2: Run startup/shutdown wiring tests**

Run:

```bash
go test ./cmd/server -count=1
```

Expected:
- PASS

**Step 3: Run the packages most exposed to the seam change**

Run:

```bash
go test ./internal/gateway ./internal/service ./internal/router -count=1
```

Expected:
- PASS

**Step 4: Review the final diff**

Run:

```bash
git diff --stat
git diff -- internal/cluster cmd/server
```

Expected:
- changes stay confined to the instance-state seam, route-store filtering, helper signatures, and tests
- no broad registry abstraction appears in the diff

**Step 5: Commit final verification checkpoint**

```bash
git add internal/cluster cmd/server
git commit -m "test: verify service-discovery-ready interface split"
```

---

## Decision Summary

- Keep `RouteStore` concrete and Redis-backed.
- Keep `PushBus` concrete behind its existing interface.
- Add `InstanceStateReader` and `InstanceStatePublisher` as the future service-discovery seam.
- Inject instance-state reads into `RouteStore` instead of letting `RouteStore` read Redis instance keys directly.
- Keep consumers on small interfaces such as `RemoteRouteReader`, `routeMirrorStore`, `PresenceRouteReader`, and `InstanceRouteReconciler`.
- Do not introduce a broad `RouteRegistry`.

## Risks To Watch

- Accidentally changing startup order while rewiring interfaces.
- Replacing small interfaces with a larger generic cluster interface.
- Letting `RouteStore` remain nil-tolerant in ways that hide missing instance-state wiring in multi-instance mode.
- Regressing presence semantics by treating `routeable=false` as offline.
- Regressing shutdown correctness if interface rewiring bypasses `SyncNow(ctx)` on lifecycle transitions.

## Implementation Notes

- Preserve the current route-only invariants from the existing multi-instance design.
- Treat this as a seam extraction, not a backend migration.
- Prefer compile-time assertions and stub-based tests over over-engineered mocks.
- If a concrete helper only exists to satisfy a single call site, remove it instead of adding another wrapper.
