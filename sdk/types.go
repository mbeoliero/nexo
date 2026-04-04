package sdk

// Response represents the standard API response
type Response struct {
	Code    int            `json:"code"`
	Msg     string         `json:"msg,omitempty"`
	Message string         `json:"message,omitempty"`
	Data    interface{}    `json:"data,omitempty"`
	Meta    map[string]any `json:"meta,omitempty"`
}

func (r Response) ErrorText() string {
	if r.Message != "" {
		return r.Message
	}
	return r.Msg
}

// UserInfo represents public user info
type UserInfo struct {
	Id        string  `json:"id"`
	Nickname  string  `json:"nickname"`
	Avatar    string  `json:"avatar"`
	Extra     *string `json:"extra,omitempty"`
	CreatedAt int64   `json:"created_at"`
}

// MessageContent represents the content of a message
type MessageContent struct {
	Text   string `json:"text,omitempty"`
	Image  string `json:"image,omitempty"`
	Video  string `json:"video,omitempty"`
	Audio  string `json:"audio,omitempty"`
	File   string `json:"file,omitempty"`
	Custom string `json:"custom,omitempty"`
}

// MessageInfo represents message info
type MessageInfo struct {
	Id             int64          `json:"id"`
	ConversationId string         `json:"conversation_id"`
	Seq            int64          `json:"seq"`
	ClientMsgId    string         `json:"client_msg_id"`
	SenderId       string         `json:"sender_id"`
	SessionType    int32          `json:"session_type"`
	MsgType        int32          `json:"msg_type"`
	Content        MessageContent `json:"content"`
	SendAt         int64          `json:"send_at"`
}

// ConversationInfo represents conversation info
type ConversationInfo struct {
	ConversationId   string `json:"conversation_id"`
	ConversationType int32  `json:"conversation_type"`
	PeerUserId       string `json:"peer_user_id,omitempty"`
	GroupId          string `json:"group_id,omitempty"`
	RecvMsgOpt       int32  `json:"recv_msg_opt"`
	IsPinned         bool   `json:"is_pinned"`
	UnreadCount      int64  `json:"unread_count"`
	MaxSeq           int64  `json:"max_seq"`
	ReadSeq          int64  `json:"read_seq"`
	UpdatedAt        int64  `json:"updated_at"`
}

// GroupInfo represents group info
type GroupInfo struct {
	Id            string `json:"id"`
	Name          string `json:"name"`
	Introduction  string `json:"introduction"`
	Avatar        string `json:"avatar"`
	Status        int32  `json:"status"`
	CreatorUserId string `json:"creator_user_id"`
	MemberCount   int64  `json:"member_count"`
	CreatedAt     int64  `json:"created_at"`
}

// GroupMember
type GroupMember struct {
	Id            int64   `json:"id"`
	GroupId       string  `json:"group_id"`
	UserId        string  `json:"user_id"`
	GroupNickname string  `json:"group_nickname"`
	GroupAvatar   string  `json:"group_avatar"`
	Extra         *string `json:"extra,omitempty"`
	RoleLevel     int32   `json:"role_level"`
	Status        int32   `json:"status"`
	JoinedAt      int64   `json:"joined_at"`
	JoinSeq       int64   `json:"join_seq"`
	InviterUserId string  `json:"inviter_user_id"`
	CreatedAt     int64   `json:"created_at"`
	UpdatedAt     int64   `json:"updated_at"`
}

// OnlineStatus
type OnlineStatus struct {
	UserId   string `json:"user_id"`
	Status   int    `json:"status"`
	Platform string `json:"platform,omitempty"`
}

type PlatformStatusDetail struct {
	PlatformId   int    `json:"platform_id"`
	PlatformName string `json:"platform_name"`
	ConnId       string `json:"conn_id"`
}

type OnlineStatusResult struct {
	UserId               string                  `json:"user_id"`
	Status               int                     `json:"status"`
	DetailPlatformStatus []*PlatformStatusDetail `json:"detail_platform_status,omitempty"`
}

type UsersOnlineStatusResult struct {
	Users []*OnlineStatusResult
	Meta  map[string]any
}

// ===== Request types =====

// RegisterRequest represents user registration request
type RegisterRequest struct {
	UserId   string `json:"user_id"`
	Nickname string `json:"nickname"`
	Password string `json:"password"`
	Avatar   string `json:"avatar,omitempty"`
}

// LoginRequest
type LoginRequest struct {
	UserId     string `json:"user_id"`
	Password   string `json:"password"`
	PlatformId int    `json:"platform_id"`
}

// LoginResponse represents user login response
type LoginResponse struct {
	Token    string    `json:"token"`
	UserInfo *UserInfo `json:"user_info"`
}

// UpdateUserRequest represents user update request
type UpdateUserRequest struct {
	Nickname string `json:"nickname,omitempty"`
	Avatar   string `json:"avatar,omitempty"`
	Extra    string `json:"extra,omitempty"`
}

// GetUsersInfoRequest represents batch get users info request
type GetUsersInfoRequest struct {
	UserIds []string `json:"user_ids"`
}

// GetUsersOnlineStatusRequest represents get users online status request
type GetUsersOnlineStatusRequest struct {
	UserIds []string `json:"user_ids"`
}

// CreateGroupRequest represents group creation request
type CreateGroupRequest struct {
	Name         string   `json:"name"`
	Introduction string   `json:"introduction,omitempty"`
	Avatar       string   `json:"avatar,omitempty"`
	MemberIds    []string `json:"member_ids,omitempty"`
}

// JoinGroupRequest represents join group request
type JoinGroupRequest struct {
	GroupId   string `json:"group_id"`
	InviterId string `json:"inviter_id,omitempty"`
}

// QuitGroupRequest represents quit group request
type QuitGroupRequest struct {
	GroupId string `json:"group_id"`
}

// SendMessageRequest represents send message request
type SendMessageRequest struct {
	ClientMsgId string         `json:"client_msg_id"`
	RecvId      string         `json:"recv_id,omitempty"`  // For single chat
	GroupId     string         `json:"group_id,omitempty"` // For group chat
	SessionType int32          `json:"session_type"`
	MsgType     int32          `json:"msg_type"`
	Content     MessageContent `json:"content"`
}

// PullMessagesRequest represents pull messages request
type PullMessagesRequest struct {
	ConversationId string `json:"conversation_id"`
	BeginSeq       int64  `json:"begin_seq"`
	EndSeq         int64  `json:"end_seq"`
	Limit          int    `json:"limit"`
}

// PullMessagesResponse represents pull messages response
type PullMessagesResponse struct {
	Messages []*MessageInfo `json:"messages"`
	MaxSeq   int64          `json:"max_seq"`
}

// UpdateConversationRequest represents update conversation request
type UpdateConversationRequest struct {
	RecvMsgOpt *int32 `json:"recv_msg_opt,omitempty"`
	IsPinned   *bool  `json:"is_pinned,omitempty"`
}

// MarkReadRequest represents mark read request
type MarkReadRequest struct {
	ConversationId string `json:"conversation_id"`
	ReadSeq        int64  `json:"read_seq"`
}

// MaxReadSeqResponse represents max and read seq response
type MaxReadSeqResponse struct {
	MaxSeq      int64 `json:"max_seq"`
	ReadSeq     int64 `json:"read_seq"`
	UnreadCount int64 `json:"unread_count"`
}

// MaxSeqResponse represents max seq response
type MaxSeqResponse struct {
	MaxSeq int64 `json:"max_seq"`
}

// UnreadCountResponse represents unread count response
type UnreadCountResponse struct {
	UnreadCount int64 `json:"unread_count"`
}
