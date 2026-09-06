package message

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/offlinepush"
	"github.com/mbeoliero/nexo/internal/onlinestore"
	"github.com/mbeoliero/nexo/internal/service/conv"
	"github.com/mbeoliero/nexo/msgbody"
)

type fakePusher struct {
	mu    sync.Mutex
	calls [][]string
	last  offlinepush.Notification
}

func (f *fakePusher) Push(_ context.Context, ids []string, n offlinepush.Notification) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, slices.Sorted(slices.Values(ids)))
	f.last = n
	return nil
}

func (f *fakePusher) wait(t *testing.T, svc *Service) [][]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := svc.Wait(ctx); err != nil {
		t.Fatalf("offline pushes did not finish: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

type fakeOnline struct {
	online map[string][]int
	err    error
}

func (f fakeOnline) Online(context.Context, []string) (map[string][]int, error) {
	return f.online, f.err
}

func TestOfflinePushTargets(t *testing.T) {
	svc, m, _ := setup(t)
	pusher := &fakePusher{}
	t.Cleanup(func() { pusher.wait(t, svc) })
	svc.SetOfflinePush(onlineStub{fakeOnline{online: map[string][]int{"u___1": {1}}}}, pusher)

	// Single chat: recipient offline → one push; sender never pushed.
	if _, err := svc.Send(t.Context(), SendInput{SenderId: "u___1", ClientMsgId: "c1", SessionType: 1, RecvId: "u___2", ContentType: msgbody.Text, Content: `{"text":"hello world"}`}); err != nil {
		t.Fatal(err)
	}
	calls := pusher.wait(t, svc)
	n := pusher.last
	if len(calls) != 1 || !slices.Equal(calls[0], []string{"u___2"}) || n.EventId() != conv.Single("u___1", "u___2")+":1" {
		t.Fatalf("single: %v %+v", calls, n)
	}
	// Facts only: raw content, type, sender; the push side renders text itself.
	if n.ContentType != msgbody.Text || n.Content != `{"text":"hello world"}` || n.SenderId != "u___1" || n.SendTime == 0 || n.Preview() != "hello world" {
		t.Fatalf("notification: %+v", n)
	}
	// Idempotent resend: no second push.
	if _, err := svc.Send(t.Context(), SendInput{SenderId: "u___1", ClientMsgId: "c1", SessionType: 1, RecvId: "u___2", ContentType: msgbody.Text, Content: `{}`}); err != nil {
		t.Fatal(err)
	}
	if len(pusher.wait(t, svc)) != 1 {
		t.Fatal("idempotent resend pushed again")
	}

	// Group: u___2 muted → nothing; then unmuted → pushed; u___1 (sender) excluded.
	if err := m.SetUserConversationOpt(t.Context(), "u___2", conv.Group("g1"), new(int32(1)), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Send(t.Context(), SendInput{SenderId: "u___1", ClientMsgId: "g1", SessionType: 2, GroupId: "g1", ContentType: msgbody.Image, Content: `{}`}); err != nil {
		t.Fatal(err)
	}
	if len(pusher.wait(t, svc)) != 1 {
		t.Fatal("muted recipient was pushed")
	}
	if err := m.SetUserConversationOpt(t.Context(), "u___2", conv.Group("g1"), new(int32(0)), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Send(t.Context(), SendInput{SenderId: "u___1", ClientMsgId: "g2", SessionType: 2, GroupId: "g1", ContentType: msgbody.Image, Content: `{}`}); err != nil {
		t.Fatal(err)
	}
	calls = pusher.wait(t, svc)
	if len(calls) != 2 || !slices.Equal(calls[1], []string{"u___2"}) || pusher.last.GroupId != "g1" || pusher.last.Preview() != "[图片]" {
		t.Fatalf("group: %v %+v", calls, pusher.last)
	}

	// Recipient online → no push. OnlineStore error → fail closed, no push.
	svc.SetOfflinePush(onlineStub{fakeOnline{online: map[string][]int{"u___2": {5}}}}, pusher)
	if _, err := svc.Send(t.Context(), SendInput{SenderId: "u___1", ClientMsgId: "c2", SessionType: 1, RecvId: "u___2", ContentType: msgbody.Text, Content: `{}`}); err != nil {
		t.Fatal(err)
	}
	if len(pusher.wait(t, svc)) != 2 {
		t.Fatalf("online recipient was pushed: %v", pusher.calls)
	}
	svc.SetOfflinePush(onlineStub{fakeOnline{err: errors.New("redis down")}}, pusher)
	if _, err := svc.Send(t.Context(), SendInput{SenderId: "u___1", ClientMsgId: "c3", SessionType: 1, RecvId: "u___2", ContentType: msgbody.Text, Content: `{}`}); err != nil {
		t.Fatal(err)
	}
	if len(pusher.wait(t, svc)) != 2 {
		t.Fatalf("failed lookup must not push: %v", pusher.calls)
	}
}

// onlineStub adapts the two-method fake to the full OnlineStore interface.
type onlineStub struct{ fakeOnline }

func (onlineStub) Add(context.Context, string, onlinestore.ConnRef) error     { return nil }
func (onlineStub) Remove(context.Context, string, onlinestore.ConnRef) error  { return nil }
func (onlineStub) Renew(context.Context, string, []onlinestore.ConnRef) error { return nil }
func (onlineStub) PurgeNode(context.Context, string) error                    { return nil }

func TestSendRateLimit(t *testing.T) {
	svc, _, _ := setup(t)
	svc.SetSendRateLimit(2)
	send := func(user, id string, unlimited bool) error {
		_, err := svc.Send(t.Context(), SendInput{SenderId: user, ClientMsgId: id, SessionType: 1, RecvId: "u___3", ContentType: msgbody.Text, Content: `{}`, Unlimited: unlimited})
		return err
	}
	if send("u___1", "a", false) != nil || send("u___1", "b", false) != nil {
		t.Fatal("burst must pass")
	}
	if err := send("u___1", "c", false); !errors.Is(err, errcode.ErrTooManyRequests) {
		t.Fatalf("third send: %v", err)
	}
	if send("u___2", "d", false) != nil || send("u___1", "e", true) != nil {
		t.Fatal("other user and internal channel must pass")
	}
}

// A stale roster (lost group_changed) must not leak content: the push side re-checks the visible range.
func TestOfflinePushVisibleRange(t *testing.T) {
	svc, m, _ := setup(t)
	svc.SetMemberCacheTtl(time.Minute)
	pusher := &fakePusher{}
	t.Cleanup(func() { pusher.wait(t, svc) })
	svc.SetOfflinePush(onlineStub{fakeOnline{}}, pusher)
	if _, err := svc.Send(t.Context(), SendInput{SenderId: "u___1", ClientMsgId: "v1", SessionType: 2, GroupId: "g1", ContentType: msgbody.Text, Content: `{}`}); err != nil {
		t.Fatal(err)
	}
	if calls := pusher.wait(t, svc); len(calls) != 1 || !slices.Equal(calls[0], []string{"u___2"}) {
		t.Fatalf("first: %v", calls)
	}
	// u___2 leaves at seq 1 but the cached roster still lists them.
	if err := m.SetUserConversationMaxSeq(t.Context(), "u___2", conv.Group("g1"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Send(t.Context(), SendInput{SenderId: "u___1", ClientMsgId: "v2", SessionType: 2, GroupId: "g1", ContentType: msgbody.Text, Content: `{}`}); err != nil {
		t.Fatal(err)
	}
	if len(pusher.wait(t, svc)) != 1 {
		t.Fatalf("removed member was pushed: %v", pusher.calls)
	}
}

// Callers filter Recipients in place; the cached roster must survive that.
func TestMemberCacheReturnsCopy(t *testing.T) {
	svc, _, _ := setup(t)
	svc.SetMemberCacheTtl(time.Minute)
	ev := PushEvent{SessionType: 2, GroupId: "g1"}
	first, err := svc.Recipients(t.Context(), ev)
	if err != nil || len(first) != 2 {
		t.Fatalf("recipients: %v %v", first, err)
	}
	_ = slices.DeleteFunc(first, func(string) bool { return true })
	if again, _ := svc.Recipients(t.Context(), ev); len(again) != 2 {
		t.Fatalf("cache corrupted: %v", again)
	}
}
