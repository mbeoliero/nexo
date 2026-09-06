package model

import "time"

type Group struct {
	Id           string `gorm:"primaryKey;size:64"`
	Name         string `gorm:"size:255"`
	Avatar       string `gorm:"size:1024"`
	Introduction string `gorm:"size:1024"`
	OwnerId      string `gorm:"size:64"`
	Status       int32
	Extra        string    `gorm:"type:text"`
	CreatedAt    time.Time `gorm:"autoCreateTime:false"`
	UpdatedAt    time.Time `gorm:"autoUpdateTime:false"`
}

func (Group) TableName() string { return "chat_groups" }

type GroupMember struct {
	GroupId       string `gorm:"primaryKey;size:64"`
	UserId        string `gorm:"primaryKey;size:64"`
	Role          int32
	Nickname      string `gorm:"size:255"`
	InviterUserId string `gorm:"size:64"`
	JoinedAt      time.Time
}

type Conversation struct {
	ConversationId string `gorm:"primaryKey;size:256"`
	Type           int32
	GroupId        string `gorm:"size:64"`
	MaxSeq         int64
	CreatedAt      time.Time `gorm:"autoCreateTime:false"`
	UpdatedAt      time.Time `gorm:"autoUpdateTime:false"`
}

type UserConversation struct {
	OwnerId        string `gorm:"primaryKey;size:64"`
	ConversationId string `gorm:"primaryKey;size:256"`
	Type           int32
	PeerUserId     string `gorm:"size:64"`
	GroupId        string `gorm:"size:64"`
	MinSeq         int64
	MaxSeq         int64
	ReadSeq        int64
	RecvMsgOpt     int32
	IsPinned       bool
	Extra          string    `gorm:"type:text"`
	UpdatedAt      time.Time `gorm:"autoUpdateTime:false"`
	CreatedAt      time.Time `gorm:"autoCreateTime:false"`
}
