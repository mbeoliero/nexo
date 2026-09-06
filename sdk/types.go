package sdk

// Wire types mirror docs/design.md §9; keep field tags in sync with the api handlers.

const (
	SessionTypeSingle int32 = 1
	SessionTypeGroup  int32 = 2

	ContentTypeText   int32 = 1
	ContentTypeCustom int32 = 100

	PlatformIOS        = 1
	PlatformAndroid    = 2
	PlatformWindows    = 3
	PlatformMacOS      = 4
	PlatformWeb        = 5
	PlatformMiniWeb    = 6
	PlatformLinux      = 7
	PlatformAndroidPad = 8
	PlatformIPad       = 9
	PlatformAdmin      = 10
)

type Profile struct {
	UserId    string `json:"user_id"`
	Nickname  string `json:"nickname"`
	Avatar    string `json:"avatar"`
	Extra     string `json:"extra"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type Session struct {
	UserId    string `json:"user_id"`
	Token     string `json:"token"`
	ExpiresAt int64  `json:"expires_at"`
}

type RegisterRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Nickname string `json:"nickname"`
}

type LoginRequest struct {
	Username   string `json:"username"`
	Password   string `json:"password"`
	PlatformId int    `json:"platform_id"`
}

// ProfileUpdate: nil fields are left unchanged.
type ProfileUpdate struct {
	Nickname *string `json:"nickname,omitempty"`
	Avatar   *string `json:"avatar,omitempty"`
	Extra    *string `json:"extra,omitempty"`
}

type UpsertUserRequest struct {
	Id       string `json:"id"`
	Nickname string `json:"nickname"`
	Avatar   string `json:"avatar"`
	Extra    string `json:"extra"`
}

type OnlineStatus struct {
	UserId    string `json:"user_id"`
	Online    bool   `json:"online"`
	Platforms []int  `json:"platform_ids"`
}

type GroupInfo struct {
	GroupId      string `json:"group_id"`
	Name         string `json:"name"`
	Avatar       string `json:"avatar"`
	Introduction string `json:"introduction"`
	OwnerId      string `json:"owner_id"`
	Status       int32  `json:"status"`
	Extra        string `json:"extra"`
	MemberCount  int64  `json:"member_count"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

type GroupMember struct {
	UserId        string `json:"user_id"`
	Role          int32  `json:"role"`
	Nickname      string `json:"nickname"`
	InviterUserId string `json:"inviter_user_id"`
	JoinedAt      int64  `json:"joined_at"`
}

type CreateGroupRequest struct {
	Name         string   `json:"name"`
	Avatar       string   `json:"avatar"`
	Introduction string   `json:"introduction"`
	Extra        string   `json:"extra"`
	MemberIds    []string `json:"member_ids"`
}

type SendMessageRequest struct {
	ClientMsgId string `json:"client_msg_id"`
	SessionType int32  `json:"session_type"`
	RecvId      string `json:"recv_id,omitempty"`
	GroupId     string `json:"group_id,omitempty"`
	ContentType int32  `json:"content_type"`
	Content     string `json:"content"` // JSON object as a string
	SenderRead  *bool  `json:"sender_read,omitempty"`
}

type Ack struct {
	ServerMsgId    string `json:"server_msg_id"`
	ConversationId string `json:"conversation_id"`
	Seq            int64  `json:"seq"`
	SendTime       int64  `json:"send_time"`
}

type Message struct {
	ServerMsgId    string `json:"server_msg_id"`
	ClientMsgId    string `json:"client_msg_id"`
	ConversationId string `json:"conversation_id"`
	Seq            int64  `json:"seq"`
	SessionType    int32  `json:"session_type"`
	SenderId       string `json:"sender_id"`
	RecvId         string `json:"recv_id,omitempty"`
	GroupId        string `json:"group_id,omitempty"`
	ContentType    int32  `json:"content_type"`
	Content        string `json:"content"`
	SendTime       int64  `json:"send_time"`
}

// PullRequest: 1 <= BeginSeq <= EndSeq, both inclusive; the server clips to the visible range.
type PullRequest struct {
	ConversationId string
	BeginSeq       int64
	EndSeq         int64
	Limit          int // 0 = server default
}

type PullResult struct {
	Messages []Message `json:"messages"`
	HasMore  bool      `json:"has_more"`
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

type Conversation struct {
	ConversationId string   `json:"conversation_id"`
	Type           int32    `json:"type"`
	PeerUserId     string   `json:"peer_user_id,omitempty"`
	GroupId        string   `json:"group_id,omitempty"`
	MinSeq         int64    `json:"min_seq"`
	MaxSeq         int64    `json:"max_seq"`
	ReadSeq        int64    `json:"read_seq"`
	Unread         int64    `json:"unread"`
	RecvMsgOpt     int32    `json:"recv_msg_opt"`
	IsPinned       bool     `json:"is_pinned"`
	Extra          string   `json:"extra"`
	UpdatedAt      int64    `json:"updated_at"`
	LastMessage    *Message `json:"last_message,omitempty"`
}

type ConversationList struct {
	Conversations []Conversation `json:"conversations"`
	NextCursor    string         `json:"next_cursor"`
	HasMore       bool           `json:"has_more"`
}

type ListConversationsRequest struct {
	Cursor          string
	Limit           int // 0 = server default
	WithLastMessage bool
}

// ConversationOpt: nil fields are left unchanged.
type ConversationOpt struct {
	RecvMsgOpt *int32 `json:"recv_msg_opt,omitempty"`
	IsPinned   *bool  `json:"is_pinned,omitempty"`
}
