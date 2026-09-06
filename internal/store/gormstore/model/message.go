package model

import "time"

type Message struct {
	ConversationId string `gorm:"primaryKey;size:256"`
	Seq            int64  `gorm:"primaryKey"`
	ServerMsgId    string `gorm:"size:64"`
	ClientMsgId    string `gorm:"size:64"`
	SenderId       string `gorm:"size:64"`
	RecvId         string `gorm:"size:64"`
	GroupId        string `gorm:"size:64"`
	SessionType    int32
	ContentType    int32
	Content        string `gorm:"type:text"`
	SendTime       time.Time
	CreatedAt      time.Time `gorm:"autoCreateTime:false"`
}
