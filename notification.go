package twocloud

import (
	"secondbit.org/ruid"
	"time"
)

type Notification struct {
	ID       ruid.RUID `json:"id,omitempty"`
	Nature   string    `json:"nature,omitempty"`
	Body     string    `json:"body,omitempty"`
	Unread   bool      `json:"unread,omitempty"`
	ReadBy   ruid.RUID `json:"read_by,omitempty"`
	TimeRead time.Time `json:"time_read,omitempty"`
	Sent     time.Time `json:"sent,omitempty"`
}

func (r *RequestBundle) GetNotificationsByDevice(device Device, before, after ruid.RUID, count int) ([]Notification, error) {
	return []Notification{}, nil
}

func (r *RequestBundle) GetNotificationsByUser(user User, before, after ruid.RUID, count int) ([]Notification, error) {
	return []Notification{}, nil
}

func (r *RequestBundle) GetNotification(id ruid.RUID) (Notification, error) {
	return Notification{}, nil
}
