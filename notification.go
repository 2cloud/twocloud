package twocloud

import (
	"database/sql"
	"errors"
	"github.com/lib/pq"
	"secondbit.org/pan"
	"strings"
	"time"
)

var NotificationTableCreateStatement = `CREATE TABLE notifications (
	id varchar primary key,
  user_id varchar NOT NULL,
  device_id varchar default NULL,
	nature varchar NOT NULL,
	unread bool default true,
	read_by varchar,
	time_read timestamp,
	sent timestamp default CURRENT_TIMESTAMP,
	body varchar NOT NULL);`

type Notification struct {
	ID       ID        `json:"id,omitempty"`
	Nature   string    `json:"nature,omitempty"`
	Body     string    `json:"body,omitempty"`
	Unread   bool      `json:"unread,omitempty"`
	ReadBy   ID        `json:"read_by,omitempty"`
	TimeRead time.Time `json:"time_read,omitempty"`
	Sent     time.Time `json:"sent,omitempty"`
	UserID   ID        `json:"user_id,omitempty"`
	DeviceID *ID       `json:"device_id,omitempty"`
}

func (n *Notification) fromRow(row ScannableRow) error {
	var idStr, userStr string
	var readByStr, deviceStr sql.NullString
	var timeRead pq.NullTime
	err := row.Scan(&idStr, &userStr, &deviceStr, &n.Nature, &n.Unread, &readByStr, &timeRead, &n.Sent, &n.Body)
	if err != nil {
		return err
	}
	id, err := IDFromString(idStr)
	if err != nil {
		return err
	}
	n.ID = id
	user_id, err := IDFromString(userStr)
	if err != nil {
		return err
	}
	n.UserID = user_id
	if deviceStr.Valid {
		device_id, err := IDFromString(deviceStr.String)
		if err != nil {
			return err
		}
		n.DeviceID = &device_id
	}
	n.ReadBy = ID(0)
	if readByStr.Valid {
		readBy, err := IDFromString(readByStr.String)
		if err != nil {
			return err
		}
		n.ReadBy = readBy
	}
	n.TimeRead = time.Time{}
	if timeRead.Valid {
		n.TimeRead = timeRead.Time
	}
	return nil
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

const (
	NotificationCreatedTopic = "notif.created"
	NotificationUpdatedTopic = "notif.updated"
	NotificationDeletedTopic = "notif.deleted"
	NotificationReadTopic    = "notif.read"
)

func (p *Persister) GetNotificationsByDevice(device Device, before, after ID, count int) ([]Notification, error) {
	query := pan.New()
	query.SQL = "SELECT * FROM notifications"
	query.IncludeWhere()
	query.Include("device_id=?", device.ID.String())
	query.IncludeIfNotEmpty("id < ?", before)
	query.IncludeIfNotEmpty("id > ?", after)
	query.FlushExpressions(" and ")
	query.IncludeOrder("sent DESC")
	query.IncludeLimit(count)
	rows, err := p.Database.Query(query.Generate(" "), query.Args...)
	if err != nil {
		return []Notification{}, nil
	}
	notifications := []Notification{}
	for rows.Next() {
		var notification Notification
		err = notification.fromRow(rows)
		if err != nil {
			return []Notification{}, err
		}
		notifications = append(notifications, notification)
	}
	err = rows.Err()
	return notifications, err
}

func (p *Persister) GetNotificationsByUser(user User, before, after ID, count int) ([]Notification, error) {
	query := pan.New()
	query.SQL = "SELECT * FROM notifications"
	query.IncludeWhere()
	query.Include("user_id=?", user.ID.String())
	query.Include("device_id IS NULL")
	query.IncludeIfNotEmpty("id < ?", before)
	query.IncludeIfNotEmpty("id > ?", after)
	query.FlushExpressions(" and ")
	query.IncludeOrder("sent DESC")
	query.IncludeLimit(count)
	rows, err := p.Database.Query(query.Generate(" "), query.Args...)
	if err != nil {
		return []Notification{}, nil
	}
	notifications := []Notification{}
	for rows.Next() {
		var notification Notification
		err = notification.fromRow(rows)
		if err != nil {
			return []Notification{}, err
		}
		notifications = append(notifications, notification)
	}
	err = rows.Err()
	return notifications, err
}

func (p *Persister) GetNotification(id ID) (Notification, error) {
	var notification Notification
	query := pan.New()
	query.SQL = "SELECT * FROM notifications"
	query.IncludeWhere()
	query.Include("id=?", id.String())
	row := p.Database.QueryRow(query.Generate(" "), query.Args...)
	err := notification.fromRow(row)
	if err == sql.ErrNoRows {
		err = NotificationNotFoundError
	}
	return notification, err
}

func (p *Persister) SendNotificationsToUser(user User, notifications []Notification) ([]Notification, error) {
	query := pan.New()
	for pos, _ := range notifications {
		id, err := p.GetID()
		if err != nil {
			return []Notification{}, err
		}
		notifications[pos].ID = id
		notifications[pos].UserID = user.ID
		query.SQL = "INSERT INTO notifications VALUES("
		query.Include("?", notifications[pos].ID.String())
		query.Include("?", notifications[pos].UserID.String())
		query.Include("?", nil)
		query.Include("?", notifications[pos].Nature)
		query.Include("?", notifications[pos].Unread)
		query.Include("?", notifications[pos].ReadBy)
		query.Include("?", notifications[pos].TimeRead)
		query.Include("?", notifications[pos].Sent)
		query.Include("?", notifications[pos].Body)
		query.FlushExpressions(", ")
		query.SQL += ")"
		_, err = p.Database.Exec(query.Generate(" "), query.Args...)
		if err != nil {
			return []Notification{}, err
		}
		_, nsqErr := p.Publish(NotificationCreatedTopic, []byte(id.String()))
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return notifications, nil
}

func (p *Persister) SendNotificationsToDevice(device Device, notifications []Notification) ([]Notification, error) {
	query := pan.New()
	for pos, _ := range notifications {
		id, err := p.GetID()
		if err != nil {
			return []Notification{}, err
		}
		notifications[pos].ID = id
		notifications[pos].UserID = device.UserID
		notifications[pos].DeviceID = &device.ID
		query.SQL = "INSERT INTO notifications VALUES("
		query.Include("?", notifications[pos].ID.String())
		query.Include("?", notifications[pos].UserID.String())
		query.Include("?", notifications[pos].DeviceID.String())
		query.Include("?", notifications[pos].Nature)
		query.Include("?", notifications[pos].Unread)
		query.Include("?", notifications[pos].ReadBy)
		query.Include("?", notifications[pos].TimeRead)
		query.Include("?", notifications[pos].Sent)
		query.Include("?", notifications[pos].Body)
		query.FlushExpressions(", ")
		query.SQL += ")"
		_, err = p.Database.Exec(query.Generate(" "), query.Args...)
		if err != nil {
			return []Notification{}, err
		}
		_, nsqErr := p.Publish(NotificationCreatedTopic, []byte(id.String()))
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return notifications, nil
}

func (p *Persister) BroadcastNotifications(notifications []Notification, filter *BroadcastFilter) ([]Notification, error) {
	if filter == nil || !filter.IsValid() {
		return []Notification{}, InvalidBroadcastFilter
	}
	response := []Notification{}
	query := pan.New()
	switch filter.Targets {
	case "users":
		query.SQL = "SELECT * FROM users"
		query.IncludeOrder("last_active DESC")
		rows, err := p.Database.Query(query.Generate(" "))
		if err != nil {
			return []Notification{}, err
		}
		for rows.Next() {
			var user User
			err = user.fromRow(rows)
			if err != nil {
				return []Notification{}, err
			}
			notifs, err := p.SendNotificationsToUser(user, notifications)
			if err != nil {
				return []Notification{}, err
			}
			response = append(response, notifs...)
		}
	case "devices":
		query.SQL = "SELECT * FROM devices"
		if len(filter.ClientType) > 1 {
			query.IncludeWhere()
			queryKeys := make([]string, len(filter.ClientType))
			queryVals := make([]interface{}, len(filter.ClientType))
			for pos, val := range filter.ClientType {
				queryKeys[pos] = "?"
				queryVals[pos] = val
			}
			query.Include("client_type IN("+strings.Join(queryKeys, ", ")+")", queryVals...)
		} else if len(filter.ClientType) == 1 {
			query.Include("client_type=?", filter.ClientType[0])
		}
		query.IncludeOrder("last_seen DESC")
		rows, err := p.Database.Query(query.Generate(" "), query.Args...)
		if err != nil {
			return []Notification{}, err
		}
		for rows.Next() {
			var device Device
			err = device.fromRow(rows)
			if err != nil {
				return []Notification{}, err
			}
			notifs, err := p.SendNotificationsToDevice(device, notifications)
			if err != nil {
				return []Notification{}, err
			}
			response = append(response, notifs...)
		}
	}
	return response, nil
}

func (p *Persister) MarkNotificationRead(notification Notification) (Notification, error) {
	notification.Unread = false
	query := pan.New()
	query.SQL = "UPDATE notifications SET"
	query.Include("unread=?", notification.Unread)
	query.IncludeWhere()
	query.Include("id=?", notification.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err == nil {
		_, nsqErr := p.Publish(NotificationReadTopic, []byte(notification.ID.String()))
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return notification, err
}

func (p *Persister) DeleteNotificationsByDevice(device Device) error {
	return p.DeleteNotificationsByDevices([]Device{device})
}

func (p *Persister) DeleteNotificationsByDevices(devices []Device) error {
	query := pan.New()
	query.SQL = "DELETE FROM notifications"
	query.IncludeWhere()
	query.FlushExpressions(" ")
	queryKeys := make([]string, len(devices))
	queryVals := make([]interface{}, len(devices))
	for _, device := range devices {
		queryKeys = append(queryKeys, "?")
		queryVals = append(queryVals, device.ID.String())
	}
	query.Include("device_id IN("+strings.Join(queryKeys, ", ")+")", queryVals...)
	_, err := p.Database.Exec(query.Generate(" and "), query.Args...)
	if err != nil {
		return err
	}
	// BUG(paddyforan): Deleting notifications by devices won't send push notifications
	return nil
}

func (p *Persister) DeleteNotificationsByUser(user User) error {
	return p.DeleteNotificationsByUsers([]User{user})
}

func (p *Persister) DeleteNotificationsByUsers(users []User) error {
	query := pan.New()
	query.SQL = "DELETE FROM notifications"
	query.IncludeWhere()
	query.FlushExpressions(" ")
	queryKeys := make([]string, len(users))
	queryVals := make([]interface{}, len(users))
	for _, user := range users {
		queryKeys = append(queryKeys, "?")
		queryVals = append(queryVals, user.ID.String())
	}
	query.Include("user_id IN("+strings.Join(queryKeys, ", ")+")", queryVals...)
	_, err := p.Database.Exec(query.Generate(" and "), query.Args...)
	if err != nil {
		return err
	}
	// BUG(paddyforan): Deleting notifications by users won't send push notifications
	return nil
}

func (p *Persister) DeleteNotifications(notifications []Notification) error {
	query := pan.New()
	query.SQL = "DELETE FROM notifications"
	query.IncludeWhere()
	queryKeys := make([]string, len(notifications))
	queryVals := make([]interface{}, len(notifications))
	for _, notification := range notifications {
		queryKeys = append(queryKeys, "?")
		queryVals = append(queryVals, notification.ID.String())
	}
	query.Include("id IN("+strings.Join(queryKeys, ", ")+")", queryVals...)
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err != nil {
		for _, notification := range notifications {
			_, nsqErr := p.Publish(NotificationDeletedTopic, []byte(notification.ID.String()))
			if nsqErr != nil {
				p.Log.Error(nsqErr.Error())
			}
		}
	}
	return err
}

func (p *Persister) DeleteNotification(notification Notification) error {
	return p.DeleteNotifications([]Notification{notification})
}
