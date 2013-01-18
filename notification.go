package twocloud

import (
	"errors"
	"time"
)

type Notification struct {
	ID              ID        `json:"id,omitempty"`
	Nature          string    `json:"nature,omitempty"`
	Body            string    `json:"body,omitempty"`
	Unread          bool      `json:"unread,omitempty"`
	ReadBy          ID        `json:"read_by,omitempty"`
	TimeRead        time.Time `json:"time_read,omitempty"`
	Sent            time.Time `json:"sent,omitempty"`
	Destination     ID        `json:"owner,omitempty"`
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
var NotificationNotFoundError = errors.New("Notification not found.")

// TODO: query notifications
func (p *Persister) GetNotificationsByDevice(device Device, before, after ID, count int) ([]Notification, error) {
	return []Notification{}, nil
}

// TODO: query notifications
func (p *Persister) GetNotificationsByUser(user User, before, after ID, count int) ([]Notification, error) {
	return []Notification{}, nil
}

// TODO: query for notification
func (p *Persister) GetNotification(id ID) (Notification, error) {
	return Notification{}, nil
}

// TODO: insert notification
func (p *Persister) SendNotificationsToUser(user User, notification []Notification) ([]Notification, error) {
	return []Notification{}, nil
}

// TODO: insert notification
func (p *Persister) SendNotificationsToDevice(device Device, notification []Notification) ([]Notification, error) {
	return []Notification{}, nil
}

// TODO: insert notifications
func (p *Persister) BroadcastNotifications(notifications []Notification, filter *BroadcastFilter) ([]Notification, error) {
	if filter != nil {
		if !filter.IsValid() {
			return []Notification{}, InvalidBroadcastFilter
		}
	}
	return []Notification{}, nil
}

// TODO: persist change
func (p *Persister) MarkNotificationRead(notification Notification) (Notification, error) {
	return Notification{}, nil
}

// TODO: delete notification
func (p *Persister) DeleteNotification(notification Notification) error {
	return nil
}
