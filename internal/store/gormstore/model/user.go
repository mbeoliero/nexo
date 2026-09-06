package model

import "time"

type User struct {
	Id           string    `gorm:"primaryKey;size:64"`
	Username     string    `gorm:"size:64"`
	PasswordHash string    `gorm:"size:255"`
	Nickname     string    `gorm:"size:255"`
	Avatar       string    `gorm:"size:1024"`
	Extra        string    `gorm:"type:text"`
	CreatedAt    time.Time `gorm:"autoCreateTime:false"`
	UpdatedAt    time.Time `gorm:"autoUpdateTime:false"`
}
