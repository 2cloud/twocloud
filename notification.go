package twocloud

import (
	"errors"
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

type BroadcastFilter struct {
	Targets    string   `json:"targets,omitempty"`
	ClientType []string `json:"client_type,omitempty"`
}

func (b *BroadcastFilter) IsValid() bool {
	if b.Targets != "devices" && b.Targets != "users" {
		return false
	}
	for _, t := range b.ClientType {
		if t != "android_phone" && t != "android_tablet" && t != "website" && t != "chrome_extension" {
			return false
		}
	}
	return true
}

var InvalidBroadcastFilter = errors.New("Invalid broadcast filter.")

func (r *RequestBundle) GetNotificationsByDevice(device Device, before, after ruid.RUID, count int) ([]Notification, error) {
	return []Notification{}, nil
}

func (r *RequestBundle) GetNotificationsByUser(user User, before, after ruid.RUID, count int) ([]Notification, error) {
	return []Notification{}, nil
}

func (r *RequestBundle) GetNotification(id ruid.RUID) (Notification, error) {
	return Notification{}, nil
}

func (r *RequestBundle) SendNotificationsToUser(user User, notification []Notification) ([]Notification, error) {
	return []Notification{}, nil
}

func (r *RequestBundle) SendNotificationsToDevice(device Device, notification []Notification) ([]Notification, error) {
	return []Notification{}, nil
}

func (r *RequestBundle) BroadcastNotifications(notifications []Notification, filter *BroadcastFilter) ([]Notification, error) {
	if filter != nil {
		if !filter.IsValid() {
			return []Notification{}, InvalidBroadcastFilter
		}
	}
	return []Notification{}, nil
}

func (r *RequestBundle) MarkNotificationRead(notification Notification) (Notification, error) {
	return Notification{}, nil
}
