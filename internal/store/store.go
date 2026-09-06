package store

import (
	"context"
	"errors"
	"time"
)

// MaxExtraBytes is the MySQL TEXT byte ceiling, shared by all storage backends.
const MaxExtraBytes = 65535

// NowMs is the time a service stores: one per transaction, millisecond-exact so unix-ms cursors
// round-trip losslessly on PG and MySQL.
func NowMs() time.Time { return time.Now().Truncate(time.Millisecond) }

var (
	ErrNotFound  = errors.New("store: not found")
	ErrDuplicate = errors.New("store: duplicate key")
	// ErrNestedTx is returned by WithTx on the transaction-scoped Store handed to a callback.
	ErrNestedTx = errors.New("store: nested transaction")
)

type User struct {
	Id           string
	Username     string
	PasswordHash string
	Nickname     string
	Avatar       string
	Extra        string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type Store interface {
	// The Store handed to fn is transaction-scoped and borrowed: it owns neither the
	// connection pool nor the transaction, so Close on it is a no-op and WithTx on it
	// returns ErrNestedTx. fn must not call WithTx on a captured outer Store either.
	WithTx(ctx context.Context, fn func(Store) error) error
	Ping(ctx context.Context) error
	Close()

	UserStore
	GroupStore
	ConversationStore
	MessageStore
	OnlineConnStore
}

type UserStore interface {
	GetUser(ctx context.Context, id string) (*User, error)
	GetUsers(ctx context.Context, ids []string) ([]User, error)
	GetUserByUsername(ctx context.Context, username string) (*User, error)
	// CreateUser fails with ErrDuplicate on id conflict; username uniqueness is checked by the service.
	CreateUser(ctx context.Context, u *User) error
	// UpsertUser inserts or updates nickname / avatar / extra; username and password are untouched.
	UpsertUser(ctx context.Context, u *User) error
	// UpdateUserProfile sets only the non-nil fields in one statement (no read-modify-write, so
	// concurrent partial updates cannot clobber each other). ErrNotFound when the user is missing.
	UpdateUserProfile(ctx context.Context, id string, nickname, avatar, extra *string, now time.Time) error
}

const (
	GroupStatusNormal    = 0
	GroupStatusDismissed = 1

	RoleMember = 1
	RoleAdmin  = 2
	RoleOwner  = 3

	ConversationSingle = 1
	ConversationGroup  = 2
)

type Group struct {
	Id           string
	Name         string
	Avatar       string
	Introduction string
	OwnerId      string
	Status       int32
	Extra        string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type GroupMember struct {
	GroupId       string
	UserId        string
	Role          int32
	Nickname      string
	InviterUserId string
	JoinedAt      time.Time
}

// Conversation is the seq row; LockConversation takes FOR UPDATE so join / quit /
// send serialize on it.
type Conversation struct {
	ConversationId string
	Type           int32
	GroupId        string
	MaxSeq         int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type UserConversation struct {
	OwnerId        string
	ConversationId string
	Type           int32
	PeerUserId     string
	GroupId        string
	MinSeq         int64
	MaxSeq         int64 // 0 = no upper bound
	ReadSeq        int64
	RecvMsgOpt     int32
	IsPinned       bool
	Extra          string
	UpdatedAt      time.Time
	CreatedAt      time.Time
}

type GroupStore interface {
	CreateGroup(ctx context.Context, g *Group, members []GroupMember) error
	GetGroup(ctx context.Context, id string) (*Group, error)
	AddGroupMember(ctx context.Context, m *GroupMember) error
	RemoveGroupMember(ctx context.Context, groupId, userId string) (removed bool, err error)
	GetGroupMember(ctx context.Context, groupId, userId string) (*GroupMember, error)
	ListGroupMembers(ctx context.Context, groupId string) ([]GroupMember, error)
	CountGroupMembers(ctx context.Context, groupId string) (int64, error)
	ListUserGroupIds(ctx context.Context, userId string) ([]string, error)
}

type OnlineConn struct {
	ConnId      string
	UserId      string
	PlatformId  int32
	NodeId      string
	HeartbeatAt time.Time
}

type OnlineConnStore interface {
	UpsertOnlineConn(ctx context.Context, c *OnlineConn) error
	DeleteOnlineConn(ctx context.Context, connId string) error
	// RenewOnlineConns bumps heartbeat_at for the listed conn ids only; rows a failed Remove left
	// behind are not renewed and expire by TTL.
	RenewOnlineConns(ctx context.Context, nodeId string, connIds []string, now time.Time) error
	ListOnlineConns(ctx context.Context, userIds []string, since time.Time) ([]OnlineConn, error)
	DeleteOnlineConnsByNode(ctx context.Context, nodeId string) error
}

type ConversationStore interface {
	// LockConversation creates the row if missing and locks it; only meaningful inside WithTx.
	LockConversation(ctx context.Context, id string, typ int32, groupId string, now time.Time) (*Conversation, error)
	GetConversation(ctx context.Context, id string) (*Conversation, error)
	GetUserConversation(ctx context.Context, ownerId, conversationId string) (*UserConversation, error)
	// GetUserConversationRow reads membership bounds and the conversation max in one statement snapshot.
	GetUserConversationRow(ctx context.Context, ownerId, conversationId string) (*UserConversationRow, error)
	// UpsertUserConversation on conflict resets min_seq / max_seq / read_seq / updated_at (re-join semantics).
	UpsertUserConversation(ctx context.Context, uc *UserConversation) error
	// CreateUserConversations bulk-inserts new rows (group creation, design §12); rows must not exist.
	CreateUserConversations(ctx context.Context, ucs []UserConversation) error
	SetUserConversationMaxSeq(ctx context.Context, ownerId, conversationId string, maxSeq int64) error
	DeleteUserConversation(ctx context.Context, ownerId, conversationId string) error
	// SetUserConversationOpt updates only the non-nil fields and never touches updated_at:
	// preferences must not reorder the list. ErrNotFound when the row is missing.
	SetUserConversationOpt(ctx context.Context, ownerId, conversationId string, recvMsgOpt *int32, isPinned *bool) error
	// VisibleOwners filters ownerIds down to those whose visible range covers seq (push authorization, design §6.1).
	VisibleOwners(ctx context.Context, conversationId string, ownerIds []string, seq int64) ([]string, error)
	// MutedOwners filters ownerIds down to those with recv_msg_opt != 0 (no offline push).
	MutedOwners(ctx context.Context, conversationId string, ownerIds []string) ([]string, error)
}

type Message struct {
	ConversationId string
	Seq            int64
	ServerMsgId    string
	ClientMsgId    string
	SenderId       string
	RecvId         string
	GroupId        string
	SessionType    int32
	ContentType    int32
	Content        string
	SendTime       time.Time
	CreatedAt      time.Time
}

type MessageKey struct {
	ConversationId string
	Seq            int64
}

// UserConversationRow is a user_conversations row joined with conversations.max_seq.
type UserConversationRow struct {
	UserConversation
	ConvMaxSeq int64
}

// ListCursor pages by (updated_at DESC, conversation_id DESC); zero value = first page.
type ListCursor struct {
	UpdatedAt      time.Time
	ConversationId string
}

type MessageStore interface {
	GetMessageByClientId(ctx context.Context, conversationId, senderId, clientMsgId string) (*Message, error)
	// InsertMessage is ON CONFLICT DO NOTHING on both unique keys; inserted=false means a duplicate.
	// Backends may infer that from the statement changing no rows, so a driver configured to report
	// matched rows instead of changed ones (MySQL's CLIENT_FOUND_ROWS) would turn a duplicate into
	// inserted=true; gormstore.New refuses such a DSN rather than let max_seq run past a real row.
	InsertMessage(ctx context.Context, m *Message) (inserted bool, err error)
	SetConversationMaxSeq(ctx context.Context, conversationId string, maxSeq int64, now time.Time) error
	ListMessages(ctx context.Context, conversationId string, beginSeq, endSeq int64, limit int) ([]Message, error)
	GetMessages(ctx context.Context, keys []MessageKey) ([]Message, error)

	// TouchUserConversation inserts the row if missing (min_seq=1) and advances updated_at without decreasing it;
	// read_seq only moves forward (GREATEST) to readSeq, pass 0 to leave it.
	TouchUserConversation(ctx context.Context, uc *UserConversation, readSeq int64) error
	// TouchConversationMembers advances updated_at for active (max_seq=0) rows, preserving any later timestamp.
	TouchConversationMembers(ctx context.Context, conversationId string, now time.Time) error
	AdvanceReadSeq(ctx context.Context, ownerId, conversationId string, readSeq int64) error
	ListUserConversations(ctx context.Context, ownerId string, cursor ListCursor, limit int) ([]UserConversationRow, error)
}

// FirstPage is a cursor after every row; MySQL DATETIME tops out at year 9999.
func FirstPage() ListCursor {
	return ListCursor{UpdatedAt: time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)}
}
