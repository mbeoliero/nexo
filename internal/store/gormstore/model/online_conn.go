package model

import "time"

type OnlineConn struct {
	ConnId      string    `gorm:"primaryKey;size:64"`
	UserId      string    `gorm:"size:64;index"`
	PlatformId  int32     `gorm:""`
	NodeId      string    `gorm:"size:64;index"`
	HeartbeatAt time.Time `gorm:"autoUpdateTime:false"`
}

func (OnlineConn) TableName() string { return "online_conns" }
