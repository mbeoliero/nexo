package api

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strconv"
	"strings"
	"testing"
	"time"

	hconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/golang-jwt/jwt/v5"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/cache/local"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/service/conversation"
	"github.com/mbeoliero/nexo/internal/service/group"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/service/user"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
	"github.com/mbeoliero/nexo/internal/tokenstore"
)

// engineOptions selects what newEngine wires; the zero value is the external-token engine.
type engineOptions struct {
	nativeLogin    bool // native provider and /auth/* instead of external tokens
	chat           bool // group, message and conversation services
	requireTls     bool
	trustedProxies []string
}

// newEngine mounts the routes on a Hertz engine over an in-memory store and returns a signer for
// external platform-user tokens. The chat services get u___1..u___3 seeded to send between.
func newEngine(t *testing.T, opts engineOptions) (*route.Engine, func(userId int64) string) {
	t.Helper()
	c := local.New()
	t.Cleanup(func() { c.Close() })
	m := storetest.NewMem()
	if opts.chat {
		for _, id := range []string{"u___1", "u___2", "u___3"} {
			if err := m.UpsertUser(t.Context(), &store.User{Id: id, UpdatedAt: time.Now()}); err != nil {
				t.Fatal(err)
			}
		}
	}
	cfg := &config.Config{}
	cfg.Auth.DefaultPlatformId = 5
	cfg.Limits.PullPageMax, cfg.Limits.ConversationPageMax, cfg.Limits.MaxSeqsPageMax = 100, 100, 100
	cfg.InternalAuth.RequireTls = opts.requireTls
	cfg.Server.TrustedProxies = opts.trustedProxies
	trusted, err := cfg.Server.TrustedCIDRs()
	if err != nil {
		t.Fatal(err)
	}

	deps := Deps{
		Auth:     auth.Chain{auth.NewExternal([]string{"ext"}, "user")},
		Internal: auth.NewInternal([]string{"secret"}, []string{"gateway"}, 300*time.Second, c),
		User:     user.New(m, nil),
	}
	if opts.nativeLogin {
		native := auth.NewNative("s", time.Hour, tokenstore.New(c))
		deps.Auth, deps.NativeLogin, deps.User = auth.Chain{native}, true, user.New(m, native)
	}
	deps.User.SetOnlineStore(stubOnline{})
	if opts.chat {
		deps.Group = group.New(group.Adapt(m), group.NoopNotifier{}, 10)
		deps.Message = message.New(message.Adapt(m), message.NoopPublisher{}, 8192)
		deps.Conv = conversation.New(m, conversation.NoopNotifier{})
	}
	e := route.NewEngine(hconfig.NewOptions(nil))
	registerRoutes(e.Group(""), cfg, deps, trusted)

	token := func(userId int64) string {
		s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"user_id": userId, "exp": time.Now().Add(time.Hour).Unix(),
		}).SignedString([]byte("ext"))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	return e, token
}

type envelope struct {
	Code    int            `json:"code"`
	Message string         `json:"message"`
	Data    jsontext.Value `json:"data"`
}

func perform(t *testing.T, e *route.Engine, method, url, body string, headers ...ut.Header) (int, envelope) {
	t.Helper()
	w := ut.PerformRequest(e, method, url, &ut.Body{Body: strings.NewReader(body), Len: len(body)}, headers...)
	var env envelope
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatalf("bad envelope %q: %v", w.Body.String(), err)
	}
	return w.Code, env
}

func call(t *testing.T, e *route.Engine, method, url, body, token string) (int, envelope) {
	t.Helper()
	headers := []ut.Header{{Key: "Content-Type", Value: "application/json"}}
	if token != "" {
		headers = append(headers, ut.Header{Key: "Authorization", Value: "Bearer " + token})
	}
	return perform(t, e, method, url, body, headers...)
}

var nonceSeq int

func signedCall(t *testing.T, e *route.Engine, secret, method, path, query, body string, extra ...ut.Header) (int, envelope) {
	t.Helper()
	nonceSeq++
	r := auth.InternalRequest{
		Service: "gateway", Timestamp: strconv.FormatInt(time.Now().Unix(), 10),
		Nonce: "nonce-" + strconv.Itoa(nonceSeq) + "-0123456789abcdef", Method: method, RawPath: path, RawQuery: query, Body: []byte(body),
	}
	for _, h := range extra {
		switch h.Key {
		case "X-User-Id":
			r.UserId = h.Value
		case "X-Platform-Id":
			r.PlatformId = h.Value
		}
	}
	headers := []ut.Header{
		{Key: "Content-Type", Value: "application/json"},
		{Key: "X-Service-Name", Value: r.Service}, {Key: "X-Timestamp", Value: r.Timestamp}, {Key: "X-Nonce", Value: r.Nonce},
		{Key: "X-Signature", Value: auth.Sign(secret, r)},
	}
	headers = append(headers, extra...)
	url := path
	if query != "" {
		url += "?" + query
	}
	return perform(t, e, method, url, body, headers...)
}
