package server

import (
	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/gateway"
	"github.com/mbeoliero/nexo/internal/offlinepush"
	"github.com/mbeoliero/nexo/internal/service/conversation"
	"github.com/mbeoliero/nexo/internal/service/group"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/service/user"
)

// Aliases over internal types so a host can name every input and output of the services it
// gets from New (design §15.1). types_test.go fails when a service method uses a type missing here.

type (
	Config             = config.Config
	ServerConfig       = config.ServerConfig
	DbConfig           = config.DbConfig
	RedisConfig        = config.RedisConfig
	BusConfig          = config.BusConfig
	OnlineStoreConfig  = config.OnlineStoreConfig
	CacheConfig        = config.CacheConfig
	OfflinePushConfig  = config.OfflinePushConfig
	AuthConfig         = config.AuthConfig
	ExternalJwtConfig  = config.ExternalJwtConfig
	NativeAuthConfig   = config.NativeAuthConfig
	InternalAuthConfig = config.InternalAuthConfig
	WsConfig           = config.WsConfig
	LimitsConfig       = config.LimitsConfig
	LogConfig          = config.LogConfig
)

type (
	Identity      = auth.Identity
	Authenticator = auth.Authenticator
	Pusher        = offlinepush.Pusher
	Notification  = offlinepush.Notification
	GatewayStats  = gateway.Stats
)

type (
	UserService         = user.Service
	Profile             = user.Profile
	Session             = user.Session
	ProfileUpdate       = user.Update
	OnlineStatus        = user.OnlineStatus
	GroupService        = group.Service
	GroupInfo           = group.Info
	GroupMember         = group.Member
	GroupCreateInput    = group.CreateInput
	MessageService      = message.Service
	Message             = message.Message
	Ack                 = message.Ack
	SendInput           = message.SendInput
	PullInput           = message.PullInput
	PullResult          = message.PullResult
	MaxSeqItem          = message.MaxSeqItem
	MaxSeqsResult       = message.MaxSeqsResult
	PushEvent           = message.PushEvent
	ConversationService = conversation.Service
	ConversationItem    = conversation.Item
	ConversationList    = conversation.ListResult
	ConversationOpt     = conversation.Opt
)
