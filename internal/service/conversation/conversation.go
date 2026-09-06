package conversation

import (
	"context"
	"errors"
	"time"

	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/service/conv"
	"github.com/mbeoliero/nexo/internal/service/dto"
	"github.com/mbeoliero/nexo/internal/store"
)

// Notifier fans a read cursor change out to the user's other connections (phase 5/6).
type Notifier interface {
	ConversationRead(ctx context.Context, ev ReadEvent)
}

// ReadEvent fans out to the user's other devices; ReaderConnId (empty over HTTP) is skipped.
type ReadEvent struct {
	UserId         string
	ReaderConnId   string
	ConversationId string
	ReadSeq        int64
}

type NoopNotifier struct{}

type NotifierFunc func(ctx context.Context, ev ReadEvent)

func (f NotifierFunc) ConversationRead(ctx context.Context, ev ReadEvent) { f(ctx, ev) }

func (NoopNotifier) ConversationRead(context.Context, ReadEvent) {}

type Service struct {
	store  Store
	notify Notifier
}

func New(st Store, notify Notifier) *Service {
	return &Service{store: st, notify: notify}
}

// notifyTimeout bounds the post-commit read fan-out; see MarkRead.
const notifyTimeout = 5 * time.Second

// MarkRead moves read_seq forward to min(readSeq, visible max); it never moves back.
func (s *Service) MarkRead(ctx context.Context, userId, readerConnId, conversationId string, readSeq int64) (int64, error) {
	if readSeq < 1 {
		return 0, errcode.ErrInvalidParam.WithMessage("read_seq must be >= 1")
	}
	row, err := s.store.GetUserConversationRow(ctx, userId, conversationId)
	if errors.Is(err, store.ErrNotFound) {
		return 0, errcode.ErrNoPermission
	}
	if err != nil {
		return 0, errcode.ErrStoreFailed.Wrap(err)
	}
	target := min(readSeq, conv.VisibleMax(row.UserConversation, row.ConvMaxSeq))
	if target <= row.ReadSeq {
		return row.ReadSeq, nil
	}
	if err := s.store.AdvanceReadSeq(ctx, userId, conversationId, target); err != nil {
		return 0, errcode.ErrStoreFailed.Wrap(err)
	}
	// read_seq is committed: the fan-out must not die with the request (message.Send does the same).
	nctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), notifyTimeout)
	defer cancel()
	s.notify.ConversationRead(nctx, ReadEvent{UserId: userId, ReaderConnId: readerConnId, ConversationId: conversationId, ReadSeq: target})
	return target, nil
}

type Item struct {
	ConversationId string       `json:"conversation_id"`
	Type           int32        `json:"type"`
	PeerUserId     string       `json:"peer_user_id,omitempty"`
	GroupId        string       `json:"group_id,omitempty"`
	MinSeq         int64        `json:"min_seq"`
	MaxSeq         int64        `json:"max_seq"` // visible max
	ReadSeq        int64        `json:"read_seq"`
	Unread         int64        `json:"unread"`
	RecvMsgOpt     int32        `json:"recv_msg_opt"`
	IsPinned       bool         `json:"is_pinned"`
	Extra          string       `json:"extra"`
	UpdatedAt      int64        `json:"updated_at"`
	LastMessage    *dto.Message `json:"last_message,omitempty"`
}

type ListResult struct {
	Conversations []Item `json:"conversations"`
	NextCursor    string `json:"next_cursor"`
	HasMore       bool   `json:"has_more"`
}

// List pages by updated_at desc; last_message is the visible max of each row, fetched in one batch.
func (s *Service) List(ctx context.Context, userId, cursor string, limit, pageMax int, withLastMessage bool) (ListResult, error) {
	rows, next, hasMore, err := conv.ListPage(ctx, s.store, userId, cursor, limit, pageMax)
	if err != nil {
		return ListResult{}, err
	}
	out := ListResult{Conversations: make([]Item, 0, len(rows)), NextCursor: next, HasMore: hasMore}
	var keys []store.MessageKey
	for _, r := range rows {
		visibleMax := conv.VisibleMax(r.UserConversation, r.ConvMaxSeq)
		out.Conversations = append(out.Conversations, Item{
			ConversationId: r.ConversationId, Type: r.Type, PeerUserId: r.PeerUserId, GroupId: r.GroupId,
			MinSeq: r.MinSeq, MaxSeq: visibleMax, ReadSeq: r.ReadSeq, Unread: max(visibleMax-r.ReadSeq, 0),
			RecvMsgOpt: r.RecvMsgOpt, IsPinned: r.IsPinned, Extra: r.Extra, UpdatedAt: r.UpdatedAt.UnixMilli(),
		})
		if withLastMessage && visibleMax >= r.MinSeq {
			keys = append(keys, store.MessageKey{ConversationId: r.ConversationId, Seq: visibleMax})
		}
	}
	if len(keys) > 0 {
		msgs, err := s.store.GetMessages(ctx, keys)
		if err != nil {
			return ListResult{}, errcode.ErrStoreFailed.Wrap(err)
		}
		byConv := lo.SliceToMap(msgs, func(m store.Message) (string, store.Message) { return m.ConversationId, m })
		for i := range out.Conversations {
			if m, ok := byConv[out.Conversations[i].ConversationId]; ok {
				out.Conversations[i].LastMessage = new(dto.MessageFromStore(m))
			}
		}
	}
	return out, nil
}

type Opt struct {
	RecvMsgOpt *int32 `json:"recv_msg_opt"`
	IsPinned   *bool  `json:"is_pinned"`
}

func (s *Service) SetOpt(ctx context.Context, userId, conversationId string, in Opt) error {
	if in.RecvMsgOpt != nil && *in.RecvMsgOpt != 0 && *in.RecvMsgOpt != 1 {
		return errcode.ErrInvalidParam.WithMessage("recv_msg_opt must be 0 or 1")
	}
	err := s.store.SetUserConversationOpt(ctx, userId, conversationId, in.RecvMsgOpt, in.IsPinned)
	if errors.Is(err, store.ErrNotFound) {
		return errcode.ErrNoPermission
	}
	if err != nil {
		return errcode.ErrStoreFailed.Wrap(err)
	}
	return nil
}
