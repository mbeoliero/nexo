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

// Notifier fans a read cursor change out to the user's other connections.
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
	// The row's spelling is authoritative: MySQL PAD SPACE lets "sg_g1 " find sg_g1's row, and
	// the other devices only match the stored id.
	conversationId = row.ConversationId
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

func itemFromRow(r store.UserConversationRow) Item {
	visibleMax := conv.VisibleMax(r.UserConversation, r.ConvMaxSeq)
	return Item{
		ConversationId: r.ConversationId, Type: r.Type, PeerUserId: r.PeerUserId, GroupId: r.GroupId,
		MinSeq: r.MinSeq, MaxSeq: visibleMax, ReadSeq: r.ReadSeq, Unread: max(visibleMax-r.ReadSeq, 0),
		RecvMsgOpt: r.RecvMsgOpt, IsPinned: r.IsPinned, Extra: r.Extra, UpdatedAt: r.UpdatedAt.UnixMilli(),
	}
}

// lastMessageKey points at the visible max; false when the caller's visible range holds no message (§5.3).
func (i Item) lastMessageKey() (store.MessageKey, bool) {
	if i.MaxSeq < i.MinSeq {
		return store.MessageKey{}, false
	}
	return store.MessageKey{ConversationId: i.ConversationId, Seq: i.MaxSeq}, true
}

// GetKey selects one conversation; exactly one field must be set so a request cannot name two.
// peer_user_id and group_id keep the §5.1 id rule on the server: clients open a chat from a user
// profile or a group entry without reproducing the ordered "si_<a>:<b>" spelling themselves.
type GetKey struct {
	ConversationId string `json:"conversation_id,omitempty"`
	PeerUserId     string `json:"peer_user_id,omitempty"`
	GroupId        string `json:"group_id,omitempty"`
}

func (k GetKey) resolve(userId string) (string, error) {
	switch {
	case lo.Count([]bool{k.ConversationId != "", k.PeerUserId != "", k.GroupId != ""}, true) != 1:
		return "", errcode.ErrInvalidParam.WithMessage("exactly one of conversation_id, peer_user_id or group_id is required")
	case k.ConversationId != "":
		return k.ConversationId, nil
	case k.GroupId != "":
		return conv.Group(k.GroupId), nil
	case k.PeerUserId == userId:
		// Sending to yourself is rejected too, so such a row can never exist; 10001 beats a
		// misleading "no conversation yet".
		return "", errcode.ErrInvalidParam.WithMessage("peer_user_id must not be the caller")
	default:
		return conv.Single(userId, k.PeerUserId), nil
	}
}

type GetResult struct {
	Conversation Item `json:"conversation"`
}

// Get reads one conversation without paging List: opening a chat from a push, from a user profile, or
// filling in a conversation the client has not cached. A missing row is ErrConversationNotFound, the
// ordinary "no conversation yet" answer here rather than §5.8's 403.
func (s *Service) Get(ctx context.Context, userId string, key GetKey, withLastMessage bool) (GetResult, error) {
	conversationId, err := key.resolve(userId)
	if err != nil {
		return GetResult{}, err
	}
	row, err := s.store.GetUserConversationRow(ctx, userId, conversationId)
	if errors.Is(err, store.ErrNotFound) {
		return GetResult{}, errcode.ErrConversationNotFound
	}
	if err != nil {
		return GetResult{}, errcode.ErrStoreFailed.Wrap(err)
	}
	// The row's spelling is authoritative (see MarkRead): the caller echoes this id back to the
	// other conversation routes.
	item := itemFromRow(*row)
	k, ok := item.lastMessageKey()
	if !withLastMessage || !ok {
		return GetResult{Conversation: item}, nil
	}
	msgs, err := s.store.GetMessages(ctx, []store.MessageKey{k})
	if err != nil {
		return GetResult{}, errcode.ErrStoreFailed.Wrap(err)
	}
	if len(msgs) > 0 {
		item.LastMessage = new(dto.MessageFromStore(msgs[0]))
	}
	return GetResult{Conversation: item}, nil
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
		item := itemFromRow(r)
		out.Conversations = append(out.Conversations, item)
		if k, ok := item.lastMessageKey(); withLastMessage && ok {
			keys = append(keys, k)
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
