package server_test

import (
	"context"
	"errors"
	"log"
	"time"

	hzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"gorm.io/gorm"

	nexo "github.com/mbeoliero/nexo/server"
)

// Mounting nexo on a host's own Hertz engine and sharing the host's database connection.
// The example has no Output comment, so it is compiled but never run: it keeps the snippet
// honest against the API without needing a live database.
func Example_embedding() {
	ctx := context.Background()

	// The host's own connection. It must be opened with gorm.Config{TranslateError: true} —
	// the store recognises duplicate keys through gorm.ErrDuplicatedKey.
	var db *gorm.DB

	cfg := nexo.DefaultConfig()
	cfg.Db.Access = "gorm"
	cfg.Auth.Providers = []string{"external_jwt"}
	cfg.Auth.ExternalJwt.Secrets = []string{"the platform's HS256 signing key"}

	s, err := nexo.New(ctx, cfg,
		nexo.WithGormDb(db),         // Shutdown does not close a connection it did not open
		nexo.WithRoutePrefix("/im"), // /im/api/v1/**, /im/ws, /im/healthz
	)
	if err != nil {
		log.Fatal(err)
	}

	// WebSocket needs Hijack, which only the standard transport provides.
	h := hzserver.Default(hzserver.WithHostPorts(":8080"), hzserver.WithTransport(standard.NewTransporter))
	s.Mount(h.Engine)
	s.Start(ctx)
	go h.Spin()

	// In-process send: skips Bearer auth and the HTTP rate limits, still fans out over the bus
	// to every node, so WebSocket clients on other nodes receive it.
	ack, err := s.Message().Send(ctx, nexo.SendInput{
		SenderId:    "u___1",
		RecvId:      "u___2",
		SessionType: 1,
		ContentType: 1,
		ClientMsgId: "c-1",
		Content:     `{"text":"hi"}`,
		Unlimited:   true,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("seq=%d", ack.Seq)

	// Drain WS alongside HTTP; keep dependencies open until both have finished.
	sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	drained := make(chan error, 1)
	go func() { drained <- s.Drain(sctx) }()
	httpShutdownErr := h.Shutdown(sctx)
	drainErr := <-drained
	s.Close()
	if err := errors.Join(httpShutdownErr, drainErr); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
