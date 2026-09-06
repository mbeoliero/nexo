package storetest

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/internal/store"
)

func RunInsertConstraints(t *testing.T, s store.Store) {
	t.Helper()
	now := time.UnixMilli(1_700_000_100_123).UTC()

	t.Run("CreateUserConversations", func(t *testing.T) {
		row := store.UserConversation{
			OwnerId: "u___9501", ConversationId: "si_u___9501:u___9502",
			Type: store.ConversationSingle, PeerUserId: "u___9502", GroupId: "",
			MinSeq: 3, MaxSeq: 9, ReadSeq: 5, RecvMsgOpt: 1, IsPinned: true,
			Extra: `{"label":"original"}`, UpdatedAt: now, CreatedAt: now.Add(-time.Hour),
		}
		checkRow := func(t *testing.T, want store.UserConversation) {
			t.Helper()
			got, err := s.GetUserConversation(t.Context(), want.OwnerId, want.ConversationId)
			if err != nil {
				t.Fatal(err)
			}
			got.CreatedAt, got.UpdatedAt = got.CreatedAt.UTC(), got.UpdatedAt.UTC()
			if *got != want {
				t.Errorf("user conversation = %+v, want %+v", *got, want)
			}
		}

		t.Run("success", func(t *testing.T) {
			otherOwner := row
			otherOwner.OwnerId = "u___9502"
			otherConversation := row
			otherConversation.ConversationId = "si_u___9501:u___9503"
			rows := []store.UserConversation{row, otherOwner, otherConversation}
			before := slices.Clone(rows)
			if err := s.CreateUserConversations(t.Context(), rows); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(rows, before) {
				t.Error("create mutated input rows")
			}
			for _, want := range before {
				want.CreatedAt = now
				checkRow(t, want)
			}
		})

		for _, name := range []string{"existing", "within_batch"} {
			t.Run(name, func(t *testing.T) {
				original := row
				original.ConversationId = "constraints-uc-" + name
				if err := s.CreateUserConversations(t.Context(), []store.UserConversation{original}); err != nil {
					t.Fatal(err)
				}
				original.CreatedAt = now
				fresh := row
				fresh.ConversationId = original.ConversationId + "-new"
				duplicate := lo.Ternary(name == "within_batch", fresh, original)
				duplicate.Type, duplicate.PeerUserId, duplicate.GroupId = store.ConversationGroup, "", "constraints-group"
				duplicate.MinSeq, duplicate.MaxSeq, duplicate.ReadSeq = 10, 0, 9
				duplicate.RecvMsgOpt, duplicate.IsPinned, duplicate.Extra = 0, false, `{"label":"replacement"}`
				duplicate.CreatedAt, duplicate.UpdatedAt = now.Add(time.Hour), now.Add(time.Minute)
				rows := []store.UserConversation{fresh, duplicate}
				before := slices.Clone(rows)
				if err := s.CreateUserConversations(t.Context(), rows); !errors.Is(err, store.ErrDuplicate) {
					t.Errorf("duplicate create = %v, want ErrDuplicate", err)
				}
				if !slices.Equal(rows, before) {
					t.Error("rejected create mutated input rows")
				}
				checkRow(t, original)
				got, err := s.GetUserConversation(t.Context(), fresh.OwnerId, fresh.ConversationId)
				if !errors.Is(err, store.ErrNotFound) {
					t.Errorf("rejected batch left a row: %+v, %v", got, err)
				}
			})
		}

		t.Run("empty", func(t *testing.T) {
			for _, rows := range [][]store.UserConversation{nil, {}} {
				if err := s.CreateUserConversations(t.Context(), rows); err != nil {
					t.Errorf("empty create = %v", err)
				}
			}
		})
	})

	// store.MaxExtraBytes is the MySQL TEXT ceiling the services validate against: a value of
	// exactly that many opaque bytes must survive every extra column unchanged.
	t.Run("MaxExtraBytes", func(t *testing.T) {
		opaque := " \nnot JSON 中文 😀 \t"
		extra := opaque + strings.Repeat("a", store.MaxExtraBytes-len(opaque))
		user := store.User{Id: "u___9701", Extra: extra, CreatedAt: now, UpdatedAt: now}
		if err := s.UpsertUser(t.Context(), &user); err != nil {
			t.Fatal(err)
		}
		if got, err := s.GetUser(t.Context(), user.Id); err != nil || got.Extra != extra {
			t.Fatalf("user extra: %d bytes stored, %v", len(got.Extra), err)
		}

		group := store.Group{Id: "constraints-extra", Name: "extra", OwnerId: user.Id, Extra: extra, CreatedAt: now, UpdatedAt: now}
		if err := s.CreateGroup(t.Context(), &group, nil); err != nil {
			t.Fatal(err)
		}
		if got, err := s.GetGroup(t.Context(), group.Id); err != nil || got.Extra != extra {
			t.Fatalf("group extra: %d bytes stored, %v", len(got.Extra), err)
		}

		row := store.UserConversation{
			OwnerId: user.Id, ConversationId: "constraints-extra-uc", Type: store.ConversationGroup,
			GroupId: group.Id, MinSeq: 1, Extra: extra, UpdatedAt: now, CreatedAt: now,
		}
		if err := s.CreateUserConversations(t.Context(), []store.UserConversation{row}); err != nil {
			t.Fatal(err)
		}
		if got, err := s.GetUserConversation(t.Context(), row.OwnerId, row.ConversationId); err != nil || got.Extra != extra {
			t.Fatalf("user conversation extra: %d bytes stored, %v", len(got.Extra), err)
		}
	})

	t.Run("InsertMessage", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			change func(*store.Message)
			insert bool
		}{
			{name: "conversation_seq", change: func(m *store.Message) { m.Seq = 1 }},
			{name: "client_msg_id", change: func(m *store.Message) { m.ClientMsgId = "original" }},
			{name: "server_same_conversation", change: func(m *store.Message) { m.ServerMsgId += "-original" }},
			{name: "server_cross_conversation", change: func(m *store.Message) {
				m.ServerMsgId += "-original"
				m.ConversationId += "-other"
			}},
			{name: "client_other_sender", change: func(m *store.Message) {
				m.ClientMsgId, m.SenderId = "original", "u___9603"
			}, insert: true},
			{name: "client_other_conversation", change: func(m *store.Message) {
				m.ClientMsgId = "original"
				m.ConversationId += "-other"
			}, insert: true},
			{name: "seq_other_conversation", change: func(m *store.Message) {
				m.Seq = 1
				m.ConversationId += "-other"
			}, insert: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				prefix := "constraints-" + tc.name
				original := store.Message{
					ConversationId: prefix, Seq: 1, ServerMsgId: prefix + "-original", ClientMsgId: "original",
					SenderId: "u___9601", RecvId: "u___9602", GroupId: "", SessionType: store.ConversationSingle,
					ContentType: 1, Content: `{"text":"original"}`, SendTime: now, CreatedAt: now,
				}
				if inserted, err := s.InsertMessage(t.Context(), &original); err != nil || !inserted {
					t.Fatalf("seed message = %v, %v", inserted, err)
				}
				candidate := original
				candidate.Seq, candidate.ServerMsgId, candidate.ClientMsgId = 2, prefix, "candidate"
				candidate.ContentType, candidate.Content = 2, `{"url":"https://example.test/image.png"}`
				candidate.SendTime, candidate.CreatedAt = now.Add(time.Second), now.Add(time.Second)
				tc.change(&candidate)
				before := candidate
				if inserted, err := s.InsertMessage(t.Context(), &candidate); err != nil || inserted != tc.insert {
					t.Errorf("insert message = %v, %v; want %v, nil", inserted, err, tc.insert)
				}
				if candidate != before {
					t.Error("insert mutated input message")
				}
				want := []store.Message{original}
				if tc.insert {
					want = append(want, before)
				}
				conversations := slices.Compact([]string{original.ConversationId, before.ConversationId})
				got := []store.Message{}
				for _, conversationId := range conversations {
					rows, err := s.ListMessages(t.Context(), conversationId, 1, 2, 10)
					if err != nil {
						t.Fatal(err)
					}
					for _, row := range rows {
						row.SendTime, row.CreatedAt = row.SendTime.UTC(), row.CreatedAt.UTC()
						got = append(got, row)
					}
				}
				if !slices.Equal(got, want) {
					t.Errorf("stored messages = %+v, want %+v", got, want)
				}
			})
		}
	})
}
