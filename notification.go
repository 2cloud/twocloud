package twocloud

import (
	"errors"
	"time"
)

type Notification struct {
	ID              uint64    `json:"id,omitempty"`
	Nature          string    `json:"nature,omitempty"`
	Body            string    `json:"body,omitempty"`
	Unread          bool      `json:"unread,omitempty"`
	ReadBy          uint64    `json:"read_by,omitempty"`
	TimeRead        time.Time `json:"time_read,omitempty"`
	Sent            time.Time `json:"sent,omitempty"`
	Destination     uint64    `json:"owner,omitempty"`
	DestinationType string    `json:"owner_type,omitempty"`
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
		for _, clientType := range validClientTypes {
			if t == clientType {
				continue
			}
		}
		return false
	}
	return true
}

var InvalidBroadcastFilter = errors.New("Invalid broadcast filter.")

func (r *RequestBundle) GetNotificationsByDevice(device Device, before, after uint64, count int) ([]Notification, error) {
	return []Notification{}, nil
}

func (r *RequestBundle) GetNotificationsByUser(user User, before, after uint64, count int) ([]Notification, error) {
	return []Notification{}, nil
}

func (r *RequestBundle) GetNotification(id uint64) (Notification, error) {
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

func (r *RequestBundle) DeleteNotification(notification Notification) error {
	return nil
}
