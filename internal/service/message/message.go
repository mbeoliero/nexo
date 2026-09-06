package message

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"uuid"

	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/identity"
	"github.com/mbeoliero/nexo/internal/offlinepush"
	"github.com/mbeoliero/nexo/internal/onlinestore"
	"github.com/mbeoliero/nexo/internal/ratelimit"
	"github.com/mbeoliero/nexo/internal/service/conv"
	"github.com/mbeoliero/nexo/internal/service/dto"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/msgbody"
)

// Publisher receives the committed message; the bus fans it out to connected clients in phase 5/6.
type Publisher interface {
	Publish(ctx context.Context, ev PushEvent)
}

type NoopPublisher struct{}

type PublisherFunc func(context.Context, PushEvent)

func (f PublisherFunc) Publish(ctx context.Context, ev PushEvent) { f(ctx, ev) }

func (NoopPublisher) Publish(context.Context, PushEvent) {}

type PushEvent struct {
	ConversationId string
	SessionType    int32
	SenderId       string
	SenderConnId   string
	RecvId         string
	GroupId        string
	Message        Message
}

type Service struct {
	store      Store
	pub        Publisher
	maxContent int
	now        func() time.Time

	// Set* are exported for an embedding host (server.New), which may call them from another
	// goroutine after Start; Send and the push workers read them on every message.
	members   atomic.Pointer[memberCache]
	push      atomic.Pointer[offlineTarget]
	sendLimit atomic.Pointer[ratelimit.Keyed]

	// Offline pushes run in the background, at most offlinePushWorkers at a time; Wait joins them
	// at shutdown so nothing touches the store after it closed.
	pushSem     chan struct{}
	pushWg      sync.WaitGroup
	pushDropped atomic.Int64
}

const offlinePushWorkers = 64

// publishTimeout bounds the post-commit bus publish; see Send.
const publishTimeout = 5 * time.Second

// Cap on distinct tracked senders (~150 B each, so ~15 MB at the cap). Senders are authenticated
// and must hold a connection or a valid token, so this is a memory backstop rather than the abuse
// guard maxTrackedIps is; past it the extra senders share one bucket.
const maxTrackedSenders = 100000

// SetSendRateLimit enforces limits.message_send_per_min per user across HTTP and WS
// (node-local; SendInput.Unlimited bypasses it for the internal channel).
func (s *Service) SetSendRateLimit(perMin int) {
	s.sendLimit.Store(ratelimit.NewKeyed(float64(perMin)/60, perMin, maxTrackedSenders))
}

// offlineTarget keeps the two SetOfflinePush halves in one word: a reader must never see the
// pusher without the OnlineStore that decides who is offline.
type offlineTarget struct {
	online onlinestore.OnlineStore
	pusher offlinepush.Pusher
}

// SetOfflinePush enables app pushes for offline recipients (design §8.9). A nil pusher leaves the
// feature off: Send then skips the presence lookup entirely rather than computing it for nobody.
func (s *Service) SetOfflinePush(online onlinestore.OnlineStore, pusher offlinepush.Pusher) {
	if pusher == nil {
		s.push.Store(nil)
		return
	}
	s.push.Store(&offlineTarget{online: online, pusher: pusher})
}

func New(st Store, pub Publisher, maxContentBytes int) *Service {
	s := &Service{store: st, pub: pub, maxContent: maxContentBytes, now: store.NowMs,
		pushSem: make(chan struct{}, offlinePushWorkers)}
	s.members.Store(newMemberCache(0))
	return s
}

type Message = dto.Message

func FromStore(m store.Message) Message { return dto.MessageFromStore(m) }

type Ack struct {
	ServerMsgId    string `json:"server_msg_id"`
	ConversationId string `json:"conversation_id"`
	Seq            int64  `json:"seq"`
	SendTime       int64  `json:"send_time"`
}

type SendInput struct {
	SenderId     string
	SenderConnId string // empty for HTTP / internal: the sender's own connections all get the push
	ClientMsgId  string
	SessionType  int32
	RecvId       string
	GroupId      string
	ContentType  int32
	Content      string
	SenderRead   bool
	Unlimited    bool // internal channel: no per-user send limit
}

func (s *Service) Send(ctx context.Context, in SendInput) (Ack, error) {
	if err := s.validate(in); err != nil {
		return Ack{}, err
	}
	var conversationId string
	switch in.SessionType {
	case store.ConversationSingle:
		if _, err := s.store.GetUser(ctx, in.RecvId); errors.Is(err, store.ErrNotFound) {
			return Ack{}, errcode.ErrUserNotFound
		} else if err != nil {
			return Ack{}, errcode.ErrStoreFailed.Wrap(err)
		}
		// recv_id/group_id are client-supplied; the field belonging to the other session type is
		// never validated, so drop it before it reaches the conversation row, the message row
		// and the push event.
		in.GroupId = ""
		conversationId = conv.Single(in.SenderId, in.RecvId)
	case store.ConversationGroup:
		g, err := s.store.GetGroup(ctx, in.GroupId)
		if errors.Is(err, store.ErrNotFound) {
			return Ack{}, errcode.ErrGroupNotFound
		} else if err != nil {
			return Ack{}, errcode.ErrStoreFailed.Wrap(err)
		}
		in.RecvId = ""
		in.GroupId = g.Id
		conversationId = conv.Group(in.GroupId)
	}

	// Fast path: same (conversation, sender, client_msg_id) already committed. It runs before the
	// rate limit so a retry for a lost ACK always gets the original ACK (design §5.4).
	if m, err := s.store.GetMessageByClientId(ctx, conversationId, in.SenderId, in.ClientMsgId); err == nil {
		return ack(*m), nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return Ack{}, errcode.ErrStoreFailed.Wrap(err)
	}
	now := s.now()
	// Limit on arrival time; a future persisted timestamp must not refill the bucket.
	if limit := s.sendLimit.Load(); !in.Unlimited && limit != nil && !limit.Allow(in.SenderId, now) {
		return Ack{}, errcode.ErrTooManyRequests.WithMessage("message send rate limit")
	}

	msg := store.Message{
		ConversationId: conversationId, ServerMsgId: uuid.NewV7().String(), ClientMsgId: in.ClientMsgId, SenderId: in.SenderId,
		RecvId: in.RecvId, GroupId: in.GroupId, SessionType: in.SessionType, ContentType: in.ContentType, Content: in.Content,
	}
	var duplicate bool
	err := s.store.WithTx(ctx, func(tx Tx) error {
		c, err := tx.LockConversation(ctx, conversationId, in.SessionType, in.GroupId, now)
		if err != nil {
			return err
		}
		if in.SessionType == store.ConversationGroup {
			if err := s.checkMembership(ctx, tx, in.GroupId, in.SenderId); err != nil {
				return err
			}
		}
		now = lo.Ternary(c.UpdatedAt.After(now), c.UpdatedAt, now)
		msg.SendTime, msg.CreatedAt = now, now
		msg.Seq = c.MaxSeq + 1
		inserted, err := tx.InsertMessage(ctx, &msg)
		if err != nil {
			return err
		}
		if !inserted {
			duplicate = true
			return errRollback // seq not consumed
		}
		if err := tx.SetConversationMaxSeq(ctx, conversationId, msg.Seq, now); err != nil {
			return err
		}
		return s.touchConversations(ctx, tx, in, conversationId, msg.Seq, now)
	})
	switch {
	case duplicate:
		m, err := s.store.GetMessageByClientId(ctx, conversationId, in.SenderId, in.ClientMsgId)
		if errors.Is(err, store.ErrNotFound) {
			// InsertMessage reports every unique key the same way, so no row under this
			// client_msg_id means the collision was on (conversation_id, seq) or server_msg_id:
			// max_seq is behind the real rows and the next send collides too. Not a retry.
			return Ack{}, errcode.ErrSeqAllocFailed.Wrap(err)
		} else if err != nil {
			return Ack{}, errcode.ErrStoreFailed.Wrap(err)
		}
		return ack(*m), nil
	case err != nil:
		return Ack{}, errcode.Or(err, errcode.ErrMessageSendFailed)
	}

	ev := PushEvent{
		ConversationId: conversationId, SessionType: in.SessionType, SenderId: in.SenderId, SenderConnId: in.SenderConnId,
		RecvId: in.RecvId, GroupId: in.GroupId, Message: FromStore(msg),
	}
	// The message is committed, so the push must not die with the request; bounded so a stuck bus
	// cannot hold the handler open either.
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	s.pub.Publish(pctx, ev)
	if s.push.Load() != nil {
		// Only the sending node, only for a newly committed message, in the background.
		s.spawnOfflinePush(context.WithoutCancel(ctx), ev)
	}
	return ack(msg), nil
}

var errRollback = errors.New("message: rollback")

func (s *Service) checkMembership(ctx context.Context, tx Tx, groupId, userId string) error {
	g, err := tx.GetGroup(ctx, groupId)
	if err != nil {
		return err
	}
	if g.Status == store.GroupStatusDismissed {
		return errcode.ErrGroupDismissed
	}
	if _, err := tx.GetGroupMember(ctx, groupId, userId); errors.Is(err, store.ErrNotFound) {
		return errcode.ErrNotGroupMember
	} else if err != nil {
		return err
	}
	return nil
}

// touchConversations bumps the list sort key; sender_read decides whether the sender's own cursor advances.
func (s *Service) touchConversations(ctx context.Context, tx Tx, in SendInput, conv string, seq int64, now time.Time) error {
	senderRead := lo.Ternary(in.SenderRead, seq, 0)
	switch in.SessionType {
	case store.ConversationSingle:
		if err := tx.TouchUserConversation(ctx, &store.UserConversation{OwnerId: in.SenderId, ConversationId: conv, Type: in.SessionType, PeerUserId: in.RecvId, UpdatedAt: now}, senderRead); err != nil {
			return err
		}
		return tx.TouchUserConversation(ctx, &store.UserConversation{OwnerId: in.RecvId, ConversationId: conv, Type: in.SessionType, PeerUserId: in.SenderId, UpdatedAt: now}, 0)
	default:
		if err := tx.TouchConversationMembers(ctx, conv, now); err != nil {
			return err
		}
		if senderRead == 0 {
			return nil
		}
		return tx.AdvanceReadSeq(ctx, in.SenderId, conv, senderRead)
	}
}

func (s *Service) validate(in SendInput) error {
	switch {
	case in.ClientMsgId == "" || len(in.ClientMsgId) > 64:
		return errcode.ErrInvalidParam.WithMessage("client_msg_id: 1-64 bytes")
	case strings.TrimRightFunc(in.ClientMsgId, unicode.IsSpace) != in.ClientMsgId:
		// MySQL PAD SPACE must not turn a distinct ID into an idempotent retry.
		return errcode.ErrInvalidParam.WithMessage("client_msg_id must not end in whitespace")
	case in.SessionType == store.ConversationSingle && !identity.Valid(in.RecvId):
		return errcode.ErrInvalidParam.WithMessage("recv_id is required for single chat")
	case in.SessionType == store.ConversationSingle && in.RecvId == in.SenderId:
		return errcode.ErrInvalidParam.WithMessage("cannot message yourself")
	case in.SessionType == store.ConversationGroup && in.GroupId == "":
		return errcode.ErrInvalidParam.WithMessage("group_id is required for group chat")
	case in.SessionType != store.ConversationSingle && in.SessionType != store.ConversationGroup:
		return errcode.ErrInvalidParam.WithMessage("session_type must be 1 or 2")
	case !msgbody.ValidType(in.ContentType):
		return errcode.ErrInvalidParam.WithMessage("unknown content_type")
	case len(in.Content) > s.maxContent:
		return errcode.ErrMessageContentTooLong
	case !jsontext.Value(in.Content).IsValid():
		return errcode.ErrInvalidParam.WithMessage("content must be a JSON value")
	}
	return nil
}

func ack(m store.Message) Ack {
	return Ack{ServerMsgId: m.ServerMsgId, ConversationId: m.ConversationId, Seq: m.Seq, SendTime: m.SendTime.UnixMilli()}
}

type PullInput struct {
	UserId         string
	ConversationId string
	BeginSeq       int64
	EndSeq         int64
	Limit          int
}

type PullResult struct {
	Messages []Message `json:"messages"`
	HasMore  bool      `json:"has_more"`
}

// Pull returns messages in [begin, end] clipped to the caller's visible range; ownership is checked first.
func (s *Service) Pull(ctx context.Context, in PullInput, pageMax int) (PullResult, error) {
	if in.BeginSeq < 1 || in.EndSeq < in.BeginSeq {
		return PullResult{}, errcode.ErrInvalidParam.WithMessage("need 1 <= begin_seq <= end_seq")
	}
	limit := in.Limit
	pageMax = max(pageMax, 1) // see MaxSeqs
	if limit <= 0 || limit > pageMax {
		limit = pageMax
	}
	uc, err := s.store.GetUserConversationRow(ctx, in.UserId, in.ConversationId)
	if errors.Is(err, store.ErrNotFound) {
		return PullResult{}, errcode.ErrNoPermission
	}
	if err != nil {
		return PullResult{}, errcode.ErrStoreFailed.Wrap(err)
	}
	begin, end := conv.VisibleRange(uc.UserConversation, uc.ConvMaxSeq, in.BeginSeq, in.EndSeq)
	if begin > end {
		return PullResult{Messages: []Message{}}, nil
	}
	rows, err := s.store.ListMessages(ctx, in.ConversationId, begin, end, limit+1)
	if err != nil {
		return PullResult{}, errcode.ErrMessagePullFailed.Wrap(err)
	}
	out := PullResult{Messages: make([]Message, 0, len(rows)), HasMore: len(rows) > limit}
	for _, m := range rows[:min(len(rows), limit)] {
		out.Messages = append(out.Messages, FromStore(m))
	}
	return out, nil
}

type MaxSeqItem struct {
	ConversationId string `json:"conversation_id"`
	MaxSeq         int64  `json:"max_seq"`
	MinSeq         int64  `json:"min_seq"`
	ReadSeq        int64  `json:"read_seq"`
}

type MaxSeqsResult struct {
	Items      []MaxSeqItem `json:"items"`
	NextCursor string       `json:"next_cursor"`
	HasMore    bool         `json:"has_more"`
}

// MaxSeqs is the sync baseline: per-conversation visible max / min / read, paged like the conversation list.
func (s *Service) MaxSeqs(ctx context.Context, userId, cursor string, limit, pageMax int) (MaxSeqsResult, error) {
	rows, next, hasMore, err := conv.ListPage(ctx, s.store, userId, cursor, limit, pageMax)
	if err != nil {
		return MaxSeqsResult{}, err
	}
	out := MaxSeqsResult{Items: make([]MaxSeqItem, 0, len(rows)), NextCursor: next, HasMore: hasMore}
	for _, r := range rows {
		out.Items = append(out.Items, MaxSeqItem{ConversationId: r.ConversationId, MaxSeq: conv.VisibleMax(r.UserConversation, r.ConvMaxSeq), MinSeq: r.MinSeq, ReadSeq: r.ReadSeq})
	}
	return out, nil
}

// Recipients is the candidate set for a push: both parties of a single chat, or the
// current group roster. It is a filter only; VisibleTo authorizes (design §6.1).
func (s *Service) Recipients(ctx context.Context, ev PushEvent) ([]string, error) {
	if ev.SessionType == store.ConversationSingle {
		return []string{ev.SenderId, ev.RecvId}, nil
	}
	if ids, ok := s.members.Load().get(ev.GroupId); ok {
		return ids, nil
	}
	members, err := s.store.ListGroupMembers(ctx, ev.GroupId)
	if err != nil {
		return nil, errcode.ErrStoreFailed.Wrap(err)
	}
	ids := lo.Map(members, func(m store.GroupMember, _ int) string { return m.UserId })
	s.members.Load().set(ev.GroupId, ids)
	return ids, nil
}

func (s *Service) VisibleTo(ctx context.Context, conversationId string, userIds []string, seq int64) ([]string, error) {
	ids, err := s.store.VisibleOwners(ctx, conversationId, userIds, seq)
	if err != nil {
		return nil, errcode.ErrStoreFailed.Wrap(err)
	}
	return ids, nil
}
