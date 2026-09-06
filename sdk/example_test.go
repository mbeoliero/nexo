package sdk_test

import (
	"context"
	"log"

	"github.com/mbeoliero/nexo/sdk"
)

// Native login: register, log in, send a message, then read the conversation list.
// Neither example has an Output comment, so both are compiled but never run: they keep the
// snippets honest against the API without needing a running server.
func Example_userFlow() {
	ctx := context.Background()
	c := sdk.New("http://localhost:8080")

	if _, err := c.Register(ctx, sdk.RegisterRequest{Username: "alice", Password: "correct horse battery", Nickname: "Alice"}); err != nil {
		log.Fatal(err)
	}
	s, err := c.Login(ctx, sdk.LoginRequest{Username: "alice", Password: "correct horse battery", PlatformId: sdk.PlatformWeb})
	if err != nil {
		log.Fatal(err)
	}
	// Every later call on this client sends the token as Bearer.
	c.SetToken(s.Token)

	// client_msg_id makes the send idempotent per sender: a retry returns the first ack
	// instead of a second message.
	ack, err := c.SendText(ctx, "c-1", "u___2", "hi")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("seq=%d conversation=%s", ack.Seq, ack.ConversationId)

	list, err := c.Conversations(ctx, sdk.ListConversationsRequest{Limit: 20, WithLastMessage: true})
	if err != nil {
		log.Fatal(err)
	}
	for _, conv := range list.Conversations {
		log.Printf("%s unread=%d", conv.ConversationId, conv.Unread)
	}
}

// Server-to-server: the platform holds its own accounts and signs requests with the shared
// secret from internal_auth, so no user token is involved. AsUser names who the call acts as.
func Example_internalSend() {
	ctx := context.Background()
	c := sdk.New("http://localhost:8080", sdk.WithInternalAuth("billing", "the internal_auth shared secret"))

	// Mirror the platform's own user into nexo; safe to repeat on every login.
	if _, err := c.InternalUpsertUser(ctx, sdk.UpsertUserRequest{Id: "u___1", Nickname: "Alice"}); err != nil {
		log.Fatal(err)
	}

	ack, err := c.InternalSendMessage(ctx, sdk.SendMessageRequest{
		ClientMsgId: "invoice-2026-09-1",
		SessionType: sdk.SessionTypeSingle,
		RecvId:      "u___2",
		ContentType: sdk.ContentTypeText,
		Content:     `{"text":"your invoice is ready"}`,
	}, sdk.AsUser("u___1"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("seq=%d", ack.Seq)

	status, err := c.InternalOnlineStatus(ctx, []string{"u___1", "u___2"})
	if err != nil {
		log.Fatal(err)
	}
	for _, s := range status {
		log.Printf("%s online=%v", s.UserId, s.Online)
	}
}
