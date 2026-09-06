package server

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"
	"gorm.io/gorm"
)

func TestWithGormDbRejectsDefaultSqlc(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Auth.Providers = []string{"external_jwt"}
	cfg.Auth.ExternalJwt.Secrets = []string{"0123456789abcdef0123456789abcdef"}
	s, err := New(t.Context(), cfg, WithGormDb(&gorm.DB{}))
	if s != nil || err == nil || !strings.Contains(err.Error(), "WithGormDb requires db.access=gorm") {
		t.Fatalf("New with default sqlc and GORM: server=%v err=%v", s, err)
	}
}

// Embedding round trip on a real PG: mount under /im, hit healthz, send in-process, read over HTTP.
func TestEmbedded(t *testing.T) {
	dsn := os.Getenv("NEXO_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("NEXO_TEST_PG_DSN empty")
	}
	if os.Getenv("NEXO_TEST_DISPOSABLE") != "1" {
		t.Fatal("embedded migration requires NEXO_TEST_DISPOSABLE=1; use make test-all or a dedicated disposable database")
	}
	cfg := DefaultConfig()
	cfg.NodeId = "embed-test"
	cfg.Db.Dsn = dsn
	cfg.Auth.Native.Secret = "0123456789abcdef0123456789abcdef" // >= minSecretLen and not a published placeholder
	if err := Migrate(t.Context(), cfg.Db); err != nil {
		t.Fatal(err)
	}
	s, err := New(t.Context(), cfg, WithRoutePrefix("/im"))
	if err != nil {
		t.Fatal(err)
	}
	e := route.NewEngine(config.NewOptions(nil))
	s.Mount(e)
	s.Start(t.Context())
	defer func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}()

	w := ut.PerformRequest(e, http.MethodGet, "/im/healthz", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("healthz: %d %s", w.Code, w.Body.String())
	}

	suffix := time.Now().Format("150405.000")
	alice, err := s.User().Register(t.Context(), "alice"+suffix, "pw123456", "Alice")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.User().Register(t.Context(), "bob"+suffix, "pw123456", "Bob")
	if err != nil {
		t.Fatal(err)
	}
	ack, err := s.Message().Send(t.Context(), SendInput{SenderId: alice.Id, RecvId: bob.Id, SessionType: 1, ContentType: 1,
		ClientMsgId: "c-" + suffix, Content: `{"text":"hi"}`, Unlimited: true})
	if err != nil {
		t.Fatal(err)
	}
	if ack.Seq != 1 {
		t.Fatalf("seq = %d, want 1", ack.Seq)
	}
	sess, err := s.User().Login(t.Context(), "bob"+suffix, "pw123456", 5)
	if err != nil {
		t.Fatal(err)
	}
	w = ut.PerformRequest(e, http.MethodGet, "/im/api/v1/conversation/list", nil, ut.Header{Key: "Authorization", Value: "Bearer " + sess.Token})
	if w.Code != http.StatusOK {
		t.Fatalf("conversation/list: %d %s", w.Code, w.Body.String())
	}
	var resp struct {
		Code int              `json:"code"`
		Data ConversationList `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Code != 0 || len(resp.Data.Conversations) != 1 || resp.Data.Conversations[0].Unread != 1 {
		t.Fatalf("unexpected list: %s", w.Body.String())
	}
}
