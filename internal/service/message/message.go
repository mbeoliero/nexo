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

	"github.com/mbeoliero/kit/log"
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

// Publisher receives the committed message; the bus fans it out to connected clients.
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

	// Dependencies are fixed at construction; the cache and limiter synchronize their own state.
	members   *memberCache
	online    onlinestore.OnlineStore
	pusher    offlinepush.Pusher
	sendLimit *ratelimit.Keyed

	// Offline pushes run in the background, at most offlinePushWorkers at a time; Wait joins them
	// at shutdown so nothing touches the store after it closed.
	pushSem     chan struct{}
	pushWg      sync.WaitGroup
	pushDropped atomic.Int64

	republished atomic.Int64
}

const offlinePushWorkers = 64

// publishTimeout bounds the post-commit bus publish; see Send.
const publishTimeout = 5 * time.Second

// Cap on distinct tracked senders (~150 B each, so ~15 MB at the cap). Senders are authenticated
// and must hold a connection or a valid token, so this is a memory backstop rather than the abuse
// guard maxTrackedIps is; past it the extra senders share one bucket.
const maxTrackedSenders = 100000

// RepublishCount includes failed attempts, but not retries skipped by the send limit.
func (s *Service) RepublishCount() int64 { return s.republished.Load() }

type Config struct {
	MaxContentBytes int
	MemberCacheTtl  time.Duration
	SendPerMin      int
	Online          onlinestore.OnlineStore
	Pusher          offlinepush.Pusher
}

func New(st Store, pub Publisher, cfg Config) *Service {
	return &Service{
		store: st, pub: pub, maxContent: cfg.MaxContentBytes, now: store.NowMs,
		members: newMemberCache(cfg.MemberCacheTtl),
		online:  cfg.Online, pusher: cfg.Pusher,
		sendLimit: ratelimit.NewKeyed(float64(cfg.SendPerMin)/60, cfg.SendPerMin, maxTrackedSenders),
		pushSem:   make(chan struct{}, offlinePushWorkers),
	}
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
	conversationId, group, err := s.normalizeDestination(ctx, &in)
	if err != nil {
		return Ack{}, err
	}

	// A retry always gets the committed ACK; only its republish is authorized and consumes send quota.
	if m, err := s.store.GetMessageByClientId(ctx, conversationId, in.SenderId, in.ClientMsgId); err == nil {
		if s.republishAllowed(ctx, in, group) {
			if in.Unlimited || s.sendLimit.Allow(in.SenderId, s.now()) {
				s.republish(ctx, *m, in.SenderConnId)
			}
		}
		return ack(*m), nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return Ack{}, errcode.ErrStoreFailed.Wrap(err)
	}
	now := s.now()
	// Limit on arrival time; a future persisted timestamp must not refill the bucket.
	if !in.Unlimited && !s.sendLimit.Allow(in.SenderId, now) {
		return Ack{}, errcode.ErrTooManyRequests.WithMessage("message send rate limit")
	}

	msg := store.Message{
		ConversationId: conversationId, ServerMsgId: uuid.NewV7().String(), ClientMsgId: in.ClientMsgId, SenderId: in.SenderId,
		RecvId: in.RecvId, GroupId: in.GroupId, SessionType: in.SessionType, ContentType: in.ContentType, Content: in.Content,
	}
	var duplicate bool
	err = s.store.WithTx(ctx, func(tx Tx) error {
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
		// This request already consumed its send quota before entering the transaction.
		s.republish(ctx, *m, in.SenderConnId)
		return ack(*m), nil
	case err != nil:
		return Ack{}, errcode.Or(err, errcode.ErrMessageSendFailed)
	}

	ev := s.publish(ctx, msg, in.SenderConnId)
	if s.pusher != nil {
		// Only the sending node, only for a newly committed message, in the background.
		s.spawnOfflinePush(context.WithoutCancel(ctx), ev)
	}
	return ack(msg), nil
}

// Use the stored spelling for routing and discard the other session type's unvalidated field
// before the idempotency lookup, transaction and push can observe it. Send validates the type first.
// The group row read here is handed back so republishAllowed does not read it a second time; it is
// nil for a single chat.
func (s *Service) normalizeDestination(ctx context.Context, in *SendInput) (string, *store.Group, error) {
	if in.SessionType == store.ConversationSingle {
		u, err := s.store.GetUser(ctx, in.RecvId)
		if errors.Is(err, store.ErrNotFound) {
			return "", nil, errcode.ErrUserNotFound
		}
		if err != nil {
			return "", nil, errcode.ErrStoreFailed.Wrap(err)
		}
		in.RecvId, in.GroupId = u.Id, ""
		return conv.Single(in.SenderId, in.RecvId), nil, nil
	}
	g, err := s.store.GetGroup(ctx, in.GroupId)
	if errors.Is(err, store.ErrNotFound) {
		return "", nil, errcode.ErrGroupNotFound
	}
	if err != nil {
		return "", nil, errcode.ErrStoreFailed.Wrap(err)
	}
	in.GroupId, in.RecvId = g.Id, ""
	return conv.Group(in.GroupId), g, nil
}

// republishAllowed re-checks a group sender before an idempotent hit publishes the stored message
// again, so someone removed from the group cannot keep fanning out to it by retrying (design §8.4).
// It runs before the quota check, so a denied republish costs no quota, and outside the conversation
// row lock, so a removal racing a retry can still let one event through; recipients drop it by
// (conversation_id, seq). g is the row normalizeDestination read microseconds earlier in this same
// request: §8.4 already concedes this re-check races a concurrent departure, so re-reading the group
// would buy no freshness and only doubles the group-table read rate under a retry storm — the
// membership row is still read now. A store failure denies the republish; the committed ACK is
// returned either way.
func (s *Service) republishAllowed(ctx context.Context, in SendInput, g *store.Group) bool {
	if in.SessionType != store.ConversationGroup {
		return true
	}
	switch err := s.checkGroupSend(ctx, s.store, g, in.SenderId); {
	case err == nil:
		return true
	case errors.Is(err, errcode.ErrNotGroupMember), errors.Is(err, errcode.ErrGroupDismissed):
		return false
	default:
		log.CtxError(ctx, "republish membership group=%s sender=%s: %v",
			in.GroupId, in.SenderId, errcode.ErrStoreFailed.Wrap(err))
		return false
	}
}

func (s *Service) republish(ctx context.Context, msg store.Message, senderConnId string) {
	s.republished.Add(1)
	s.publish(ctx, msg, senderConnId)
}

func (s *Service) publish(ctx context.Context, msg store.Message, senderConnId string) PushEvent {
	ev := PushEvent{
		ConversationId: msg.ConversationId, SessionType: msg.SessionType, SenderId: msg.SenderId,
		SenderConnId: senderConnId, RecvId: msg.RecvId, GroupId: msg.GroupId, Message: FromStore(msg),
	}
	// The message is committed, so the push must not die with the request; bounded so a stuck bus
	// cannot hold the handler open either.
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	s.pub.Publish(pctx, ev)
	return ev
}

var errRollback = errors.New("message: rollback")

// checkMembership loads the group inside the caller's transaction, which for Send means under the
// conversation row lock: §8.4 closes the "kicked after the check" window only if this read is that
// fresh, so it must not be replaced by a row read before the lock.
func (s *Service) checkMembership(ctx context.Context, tx Tx, groupId, userId string) error {
	g, err := tx.GetGroup(ctx, groupId)
	if err != nil {
		return err
	}
	return s.checkGroupSend(ctx, tx, g, userId)
}

// checkGroupSend applies §8.4 step 3b's condition to an already-loaded chat_groups row.
func (s *Service) checkGroupSend(ctx context.Context, tx Tx, g *store.Group, userId string) error {
	if g.Status == store.GroupStatusDismissed {
		return errcode.ErrGroupDismissed
	}
	if _, err := tx.GetGroupMember(ctx, g.Id, userId); errors.Is(err, store.ErrNotFound) {
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
	if ids, ok := s.members.get(ev.GroupId); ok {
		return ids, nil
	}
	members, err := s.store.ListGroupMembers(ctx, ev.GroupId)
	if err != nil {
		return nil, errcode.ErrStoreFailed.Wrap(err)
	}
	ids := lo.Map(members, func(m store.GroupMember, _ int) string { return m.UserId })
	s.members.set(ev.GroupId, ids)
	return ids, nil
}

func (s *Service) VisibleTo(ctx context.Context, conversationId string, userIds []string, seq int64) ([]string, error) {
	ids, err := s.store.VisibleOwners(ctx, conversationId, userIds, seq)
	if err != nil {
		return nil, errcode.ErrStoreFailed.Wrap(err)
	}
	return ids, nil
}
