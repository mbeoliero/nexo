package group

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/service/conv"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

type recorder struct{ changed []string }

func (r *recorder) GroupChanged(_ context.Context, id string) { r.changed = append(r.changed, id) }

func setup(t *testing.T) (*Service, *storetest.Mem, *recorder) {
	t.Helper()
	m := storetest.NewMem()
	for _, id := range []string{"u___1", "u___2", "u___3", "u___4"} {
		_ = m.UpsertUser(t.Context(), &store.User{Id: id, UpdatedAt: time.Now()})
	}
	r := &recorder{}
	return New(Adapt(m), r, 3), m, r
}

func TestCreateAndVisibility(t *testing.T) {
	ctx := t.Context()
	s, m, r := setup(t)

	if _, err := s.Create(ctx, "u___1", CreateInput{Name: "", MemberIds: nil}); !errors.Is(err, errcode.ErrInvalidParam) {
		t.Fatalf("empty name: %v", err)
	}
	if _, err := s.Create(ctx, "u___1", CreateInput{Name: "x", MemberIds: []string{"u___404"}}); !errors.Is(err, errcode.ErrUserNotFound) {
		t.Fatalf("unknown member: %v", err)
	}
	if _, err := s.Create(ctx, "u___1", CreateInput{Name: "x", MemberIds: []string{"u___2", "u___3", "u___4"}}); !errors.Is(err, errcode.ErrGroupFull) {
		t.Fatalf("over limit: %v", err)
	}
	info, err := s.Create(ctx, "u___1", CreateInput{Name: "team", MemberIds: []string{"u___2", "u___1"}})
	if err != nil || info.MemberCount != 2 || info.OwnerId != "u___1" || len(info.Id) != 16 {
		t.Fatalf("create: %+v %v", info, err)
	}
	if len(r.changed) != 1 || r.changed[0] != info.Id {
		t.Fatalf("notify: %v", r.changed)
	}
	conv := conv.Group(info.Id)
	for _, id := range []string{"u___1", "u___2"} {
		uc, err := m.GetUserConversation(ctx, id, conv)
		if err != nil || uc.MinSeq != 1 || uc.MaxSeq != 0 {
			t.Fatalf("initial member %s: %+v %v", id, uc, err)
		}
	}

	// Simulate 5 messages, then join: joiner sees from seq 6 and starts read at 5.
	c, _ := m.LockConversation(ctx, conv, store.ConversationGroup, info.Id, time.Now())
	c.MaxSeq = 5
	m.SetConversation(*c)
	if err := s.Join(ctx, info.Id, "u___3"); err != nil {
		t.Fatalf("join: %v", err)
	}
	if err := s.Join(ctx, info.Id, "u___3"); !errors.Is(err, errcode.ErrAlreadyGroupMember) {
		t.Fatalf("join twice: %v", err)
	}
	if err := s.Join(ctx, info.Id, "u___4"); !errors.Is(err, errcode.ErrGroupFull) {
		t.Fatalf("join full: %v", err)
	}
	uc, _ := m.GetUserConversation(ctx, "u___3", conv)
	if uc.MinSeq != 6 || uc.ReadSeq != 5 || uc.MaxSeq != 0 {
		t.Fatalf("joiner bounds: %+v", uc)
	}

	c.MaxSeq = 9
	m.SetConversation(*c)
	if err := s.Quit(ctx, info.Id, "u___3"); err != nil {
		t.Fatalf("quit: %v", err)
	}
	uc, _ = m.GetUserConversation(ctx, "u___3", conv)
	if uc.MaxSeq != 9 || uc.MinSeq != 6 {
		t.Fatalf("quitter bounds: %+v", uc)
	}
	if err := s.Quit(ctx, info.Id, "u___3"); !errors.Is(err, errcode.ErrNotGroupMember) {
		t.Fatalf("quit twice: %v", err)
	}
	if err := s.Quit(ctx, info.Id, "u___1"); !errors.Is(err, errcode.ErrInvalidParam) {
		t.Fatalf("owner quit: %v", err)
	}

	// Re-join after more messages resets the lower bound and clears the upper bound.
	c.MaxSeq = 12
	m.SetConversation(*c)
	if err := s.Join(ctx, info.Id, "u___3"); err != nil {
		t.Fatalf("rejoin: %v", err)
	}
	uc, _ = m.GetUserConversation(ctx, "u___3", conv)
	if uc.MinSeq != 13 || uc.MaxSeq != 0 || uc.ReadSeq != 12 {
		t.Fatalf("rejoin bounds: %+v", uc)
	}
	if len(r.changed) != 4 {
		t.Fatalf("notify count: %d", len(r.changed))
	}
}

// Leaving before the first message must not leave a row whose max_seq=0 reads as "unbounded".
func TestLeaveEmptyGroupDeletesConversation(t *testing.T) {
	ctx := t.Context()
	s, m, _ := setup(t)
	info, err := s.Create(ctx, "u___1", CreateInput{Name: "team", MemberIds: []string{"u___2"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Kick(ctx, info.Id, "u___1", "u___2"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.GetUserConversation(ctx, "u___2", conv.Group(info.Id)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("row must be gone, got %v", err)
	}
	if ids, _ := m.VisibleOwners(ctx, conv.Group(info.Id), []string{"u___1", "u___2"}, 1); len(ids) != 1 || ids[0] != "u___1" {
		t.Fatalf("kicked member still visible: %v", ids)
	}
}

func TestKickAndRead(t *testing.T) {
	ctx := t.Context()
	s, m, _ := setup(t)
	info, _ := s.Create(ctx, "u___1", CreateInput{Name: "team", MemberIds: []string{"u___2", "u___3"}})
	gm, _ := m.GetGroupMember(ctx, info.Id, "u___2")
	gm.Role = store.RoleAdmin
	m.SetGroupMember(*gm)

	cases := []struct {
		op, target string
		want       error
	}{
		{"u___3", "u___2", errcode.ErrNotGroupAdmin},
		{"u___2", "u___1", errcode.ErrCannotKickOwner},
		{"u___2", "u___2", errcode.ErrInvalidParam},
		{"u___4", "u___3", errcode.ErrNotGroupMember},
		{"u___2", "u___3", nil},
		{"u___1", "u___2", nil},
	}
	for _, c := range cases {
		if err := s.Kick(ctx, info.Id, c.op, c.target); !errors.Is(err, c.want) {
			t.Errorf("kick %s->%s: got %v, want %v", c.op, c.target, err, c.want)
		}
	}
	if _, err := s.Get(ctx, info.Id, "u___3"); !errors.Is(err, errcode.ErrNotGroupMember) {
		t.Fatalf("kicked user reading group: %v", err)
	}
	got, err := s.Get(ctx, info.Id, "u___1")
	if err != nil || got.MemberCount != 1 {
		t.Fatalf("get: %+v %v", got, err)
	}
	members, err := s.Members(ctx, info.Id, "u___1")
	if err != nil || len(members) != 1 || members[0].Role != store.RoleOwner {
		t.Fatalf("members: %+v %v", members, err)
	}
	if _, err := s.Get(ctx, "nope", "u___1"); !errors.Is(err, errcode.ErrGroupNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

// Extra is opaque text capped at the MySQL TEXT ceiling; storetest.RunInsertConstraints proves
// that ceiling round-trips in every database.
func TestCreateExtraBoundary(t *testing.T) {
	ctx := t.Context()
	s, _, r := setup(t)
	extra := " \nnot JSON 中文 😀 \t"
	extra += strings.Repeat("a", store.MaxExtraBytes-len(extra))

	if _, err := s.Create(ctx, "u___1", CreateInput{Name: "extra", Extra: extra + "a"}); !errors.Is(err, errcode.ErrInvalidParam) {
		t.Fatalf("extra over the limit: %v", err)
	}
	if len(r.changed) != 0 {
		t.Fatalf("rejected create notified: %v", r.changed)
	}
	info, err := s.Create(ctx, "u___1", CreateInput{Name: "extra", Extra: extra})
	if err != nil || info.Extra != extra {
		t.Fatalf("extra at the limit (%d bytes): %v", len(extra), err)
	}
	if stored, err := s.Get(ctx, info.Id, "u___1"); err != nil || stored != info {
		t.Fatalf("stored group: %+v, %v", stored, err)
	}
}

// cancelOnCommit stands in for the request context dying the instant the write lands — a client
// that backgrounds the app, or a handler that returns, right after the membership change commits.
type cancelOnCommit struct {
	store.Store
	cancel context.CancelFunc
}

func (c cancelOnCommit) WithTx(ctx context.Context, fn func(store.Store) error) error {
	err := c.Store.WithTx(ctx, fn)
	c.cancel()
	return err
}

type ctxNotifier struct {
	calls    int
	err      error
	deadline bool
}

func (n *ctxNotifier) GroupChanged(ctx context.Context, _ string) {
	n.calls, n.err = n.calls+1, ctx.Err()
	_, n.deadline = ctx.Deadline()
}

// Membership is durable once the tx commits, so the invalidation broadcast must not be cancelled
// with the request: every other node would keep serving a stale member cache for a whole TTL —
// the new member silently gets no pushes, and a removed one keeps getting them.
func TestNotifySurvivesRequestCancellation(t *testing.T) {
	base, m, _ := setup(t)
	g, err := base.Create(t.Context(), "u___1", CreateInput{Name: "team", MemberIds: []string{"u___2"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		call func(*Service, context.Context) error
	}{
		{"create", func(s *Service, ctx context.Context) error {
			_, err := s.Create(ctx, "u___3", CreateInput{Name: "other", MemberIds: []string{"u___4"}})
			return err
		}},
		{"join", func(s *Service, ctx context.Context) error { return s.Join(ctx, g.Id, "u___3") }},
		{"remove", func(s *Service, ctx context.Context) error { return s.Quit(ctx, g.Id, "u___2") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			n := &ctxNotifier{}
			s := New(Adapt(cancelOnCommit{Store: m, cancel: cancel}), n, 3)

			if err := tc.call(s, ctx); err != nil {
				t.Fatal(err)
			}
			if ctx.Err() == nil {
				t.Fatal("the test store must have cancelled the request context")
			}
			if n.calls != 1 {
				t.Fatalf("notified %d times, want 1", n.calls)
			}
			if n.err != nil {
				t.Fatalf("notify ran on a dead context: %v", n.err)
			}
			// ...and it is still bounded, so a wedged bus cannot hold the handler open forever.
			if !n.deadline {
				t.Fatal("notify context has no deadline")
			}
		})
	}
}
