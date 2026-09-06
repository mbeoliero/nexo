package sdk_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	hconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/api"
	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/cache/local"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/service/conversation"
	"github.com/mbeoliero/nexo/internal/service/group"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/service/user"
	"github.com/mbeoliero/nexo/internal/store/storetest"
	"github.com/mbeoliero/nexo/internal/tokenstore"
	"github.com/mbeoliero/nexo/sdk"
)

// hertzHandler feeds net/http requests into a Hertz engine so httptest can front api.Register.
func hertzHandler(e *route.Engine) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var headers []ut.Header
		for k, vs := range r.Header {
			for _, v := range vs {
				headers = append(headers, ut.Header{Key: k, Value: v})
			}
		}
		rec := ut.PerformRequest(e, r.Method, r.URL.RequestURI(), &ut.Body{Body: strings.NewReader(string(raw)), Len: len(raw)}, headers...)
		resp := rec.Result()
		resp.Header.VisitAll(func(k, v []byte) { w.Header().Add(string(k), string(v)) })
		w.WriteHeader(resp.StatusCode())
		w.Write(resp.Body())
	})
}

// Mounted under /im with native login and the internal channel on, the way an embedding host would run it.
func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	c := local.New()
	t.Cleanup(func() { c.Close() })
	m := storetest.NewMem()
	native := auth.NewNative("native-secret", time.Hour, tokenstore.New(c))
	cfg := &config.Config{NodeId: "sdk-test"}
	cfg.Auth.DefaultPlatformId = 5
	cfg.Limits.PullPageMax, cfg.Limits.ConversationPageMax = 100, 100
	e := route.NewEngine(hconfig.NewOptions(nil))
	api.Register(e, "/im", cfg, api.Deps{
		Auth: auth.Chain{native}, NativeLogin: true,
		Internal: auth.NewInternal([]string{"hmac-secret"}, []string{"platform"}, 300*time.Second, c),
		User:     user.New(m, native),
		Group:    group.New(group.Adapt(m), group.NoopNotifier{}, 10),
		Message:  message.New(message.Adapt(m), message.NoopPublisher{}, 8192),
		Conv:     conversation.New(m, conversation.NoopNotifier{}),
	})
	srv := httptest.NewServer(hertzHandler(e))
	t.Cleanup(srv.Close)
	return srv
}

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientRoundTrip(t *testing.T) {
	srv := newServer(t)
	ctx := t.Context()
	alice := sdk.New(srv.URL+"/im/", sdk.WithPlatformId(sdk.PlatformWeb))
	bob := sdk.New(srv.URL + "/im")

	a, err := alice.Register(ctx, sdk.RegisterRequest{Username: "alice", Password: "pw123456", Nickname: "Alice"})
	noErr(t, err)
	b, err := bob.Register(ctx, sdk.RegisterRequest{Username: "bob", Password: "pw123456", Nickname: "Bob"})
	noErr(t, err)
	if _, err := alice.Me(ctx); sdk.CodeOf(err) != errcode.ErrTokenMissing.Code {
		t.Fatalf("me without token: %v", err)
	}
	_, err = alice.Login(ctx, sdk.LoginRequest{Username: "alice", Password: "pw123456", PlatformId: sdk.PlatformWeb})
	noErr(t, err)
	_, err = bob.Login(ctx, sdk.LoginRequest{Username: "bob", Password: "pw123456", PlatformId: sdk.PlatformIOS})
	noErr(t, err)
	me, err := alice.Me(ctx)
	noErr(t, err)
	if me.UserId != a.UserId {
		t.Fatalf("me = %+v", me)
	}
	nick := "Alice2"
	p, err := alice.UpdateMe(ctx, sdk.ProfileUpdate{Nickname: &nick})
	noErr(t, err)
	if p.Nickname != nick {
		t.Fatalf("update = %+v", p)
	}
	users, err := alice.Users(ctx, []string{a.UserId, b.UserId})
	noErr(t, err)
	if len(users) != 2 {
		t.Fatalf("users = %+v", users)
	}

	ack, err := alice.SendText(ctx, "c1", b.UserId, "hi")
	noErr(t, err)
	if ack.Seq != 1 {
		t.Fatalf("ack = %+v", ack)
	}
	again, err := alice.SendText(ctx, "c1", b.UserId, "hi")
	noErr(t, err)
	if again.ServerMsgId != ack.ServerMsgId {
		t.Fatalf("idempotent resend: %+v vs %+v", again, ack)
	}
	pulled, err := bob.Pull(ctx, sdk.PullRequest{ConversationId: ack.ConversationId, BeginSeq: 1, EndSeq: 1})
	noErr(t, err)
	if len(pulled.Messages) != 1 || pulled.Messages[0].Content != `{"text":"hi"}` {
		t.Fatalf("pull = %+v", pulled)
	}
	ms, err := bob.MaxSeqs(ctx, "", 0)
	noErr(t, err)
	if len(ms.Items) != 1 || ms.Items[0].MaxSeq != 1 {
		t.Fatalf("max_seqs = %+v", ms)
	}
	convs, err := bob.Conversations(ctx, sdk.ListConversationsRequest{WithLastMessage: true})
	noErr(t, err)
	if len(convs.Conversations) != 1 || convs.Conversations[0].Unread != 1 || convs.Conversations[0].LastMessage == nil {
		t.Fatalf("conversations = %+v", convs)
	}
	seq, err := bob.MarkRead(ctx, ack.ConversationId, 1)
	noErr(t, err)
	if seq != 1 {
		t.Fatalf("read_seq = %d", seq)
	}
	pinned := true
	if err := bob.SetConversationOpt(ctx, ack.ConversationId, sdk.ConversationOpt{IsPinned: &pinned}); err != nil {
		t.Fatal(err)
	}
	convs, err = bob.Conversations(ctx, sdk.ListConversationsRequest{})
	noErr(t, err)
	if convs.Conversations[0].Unread != 0 || !convs.Conversations[0].IsPinned {
		t.Fatalf("after read+pin = %+v", convs.Conversations[0])
	}

	g, err := alice.CreateGroup(ctx, sdk.CreateGroupRequest{Name: "g1", MemberIds: []string{b.UserId}})
	noErr(t, err)
	if g.MemberCount != 2 {
		t.Fatalf("group = %+v", g)
	}
	info, err := bob.Group(ctx, g.GroupId)
	noErr(t, err)
	if info.OwnerId != a.UserId {
		t.Fatalf("group info = %+v", info)
	}
	members, err := bob.GroupMembers(ctx, g.GroupId)
	noErr(t, err)
	if len(members) != 2 {
		t.Fatalf("members = %+v", members)
	}
	_, err = alice.SendGroupText(ctx, "g-c1", g.GroupId, "hello group")
	noErr(t, err)
	if err := bob.QuitGroup(ctx, g.GroupId); err != nil {
		t.Fatal(err)
	}
	if err := bob.JoinGroup(ctx, g.GroupId); err != nil {
		t.Fatal(err)
	}
	if err := alice.KickGroupMember(ctx, g.GroupId, b.UserId); err != nil {
		t.Fatal(err)
	}
	if e, ok := errors.AsType[*sdk.Error](bob.KickGroupMember(ctx, g.GroupId, a.UserId)); !ok || !e.IsBusiness() {
		t.Fatalf("kick owner by non-member must be a business error: %v", e)
	}
	st, err := alice.OnlineStatus(ctx, []string{b.UserId})
	noErr(t, err)
	if len(st) != 1 || st[0].Online {
		t.Fatalf("online = %+v", st)
	}

	if err := bob.Logout(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := bob.Me(ctx); sdk.CodeOf(err) != errcode.ErrTokenMissing.Code {
		t.Fatalf("after logout: %v", err)
	}
}

func TestInternalChannel(t *testing.T) {
	srv := newServer(t)
	ctx := t.Context()
	if _, err := sdk.New(srv.URL+"/im").InternalUsers(ctx, []string{"u___1"}); err == nil {
		t.Fatal("internal call without WithInternalAuth must fail")
	}
	bad := sdk.New(srv.URL+"/im", sdk.WithInternalAuth("platform", "wrong"))
	if err := bad.InternalHealth(ctx); sdk.CodeOf(err) != errcode.ErrUnauthorized.Code {
		t.Fatalf("wrong secret: %v", err)
	}
	pf := sdk.New(srv.URL+"/im", sdk.WithInternalAuth("platform", "hmac-secret"))
	if err := pf.InternalHealth(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"u___1", "u___2"} {
		_, err := pf.InternalUpsertUser(ctx, sdk.UpsertUserRequest{Id: id, Nickname: "N" + id})
		noErr(t, err)
	}
	users, err := pf.InternalUsers(ctx, []string{"u___1", "u___2"})
	noErr(t, err)
	if len(users) != 2 || users[0].Nickname != "Nu___1" {
		t.Fatalf("users = %+v", users)
	}
	ack, err := pf.InternalSendMessage(ctx, sdk.SendMessageRequest{ClientMsgId: "p1", SessionType: sdk.SessionTypeSingle, RecvId: "u___2",
		ContentType: sdk.ContentTypeCustom, Content: `{"k":"v"}`}, sdk.AsUser("u___1"), sdk.PlatformId(sdk.PlatformAdmin))
	noErr(t, err)
	if ack.Seq != 1 {
		t.Fatalf("ack = %+v", ack)
	}
	// Platform sends default to sender_read=false: the sender's own list shows it unread.
	convs, err := pf.InternalConversations(ctx, sdk.ListConversationsRequest{}, sdk.AsUser("u___1"))
	noErr(t, err)
	if len(convs.Conversations) != 1 || convs.Conversations[0].Unread != 1 {
		t.Fatalf("sender conversations = %+v", convs)
	}
	g, err := pf.InternalCreateGroup(ctx, sdk.CreateGroupRequest{Name: "pg"}, sdk.AsUser("u___1"))
	noErr(t, err)
	if err := pf.InternalJoinGroup(ctx, g.GroupId, sdk.AsUser("u___2")); err != nil {
		t.Fatal(err)
	}
	if err := pf.InternalKickGroupMember(ctx, g.GroupId, "u___2", sdk.AsUser("u___1")); err != nil {
		t.Fatal(err)
	}
	st, err := pf.InternalOnlineStatus(ctx, []string{"u___2"})
	noErr(t, err)
	if len(st) != 1 {
		t.Fatalf("online = %+v", st)
	}
	if _, err := pf.InternalSendMessage(ctx, sdk.SendMessageRequest{ClientMsgId: "p2"}, sdk.AsUser("nx__1")); sdk.CodeOf(err) != errcode.ErrInvalidParam.Code {
		t.Fatalf("native id as-user must be rejected: %v", err)
	}
}
