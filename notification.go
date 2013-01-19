package twocloud

import (
	"database/sql"
	"errors"
	"github.com/bmizerany/pq"
	"strconv"
	"strings"
	"time"
)

var NotificationTableCreateStatement = `CREATE TABLE notifications (
	id varchar primary key,
	destination varchar NOT NULL,
	destination_type varchar NOT NULL,
	nature varchar NOT NULL,
	unread bool default true,
	read_by varchar,
	time_read timestamp,
	sent timestamp default CURRENT_TIMESTAMP,
	body varchar NOT NULL);`

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

func (n *Notification) fromRow(row ScannableRow) error {
	var idStr, destinationStr string
	var readByStr sql.NullString
	var timeRead pq.NullTime
	err := row.Scan(&idStr, &destinationStr, &n.DestinationType, &n.Nature, &n.Unread, &readByStr, &timeRead, &n.Sent, &n.Body)
	if err != nil {
		return err
	}
	id, err := IDFromString(idStr)
	if err != nil {
		return err
	}
	n.ID = id
	destination, err := IDFromString(destinationStr)
	if err != nil {
		return err
	}
	n.Destination = destination
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

func (p *Persister) GetNotificationsByDevice(device Device, before, after ID, count int) ([]Notification, error) {
	var rows *sql.Rows
	var err error
	if !before.IsZero() && !after.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM notifications WHERE destination=$1 and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", device.ID.String(), before.String(), after.String(), count)
	} else if !before.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM notifications WHERE destination=$1 and id < $2 ORDER BY sent DESC LIMIT $3", device.ID.String(), before.String(), count)
	} else if !after.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM notifications WHERE destination=$1 and id > $2 ORDER BY sent DESC LIMIT $3", device.ID.String(), after.String(), count)
	} else {
		rows, err = p.Database.Query("SELECT * FROM destination WHERE sender=$1 ORDER BY sent DESC LIMIT $2", device.ID.String(), count)
	}
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
	var rows *sql.Rows
	var err error
	if !before.IsZero() && !after.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM notifications WHERE destination=$1 and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", user.ID.String(), before.String(), after.String(), count)
	} else if !before.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM notifications WHERE destination=$1 and id < $2 ORDER BY sent DESC LIMIT $3", user.ID.String(), before.String(), count)
	} else if !after.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM notifications WHERE destination=$1 and id > $2 ORDER BY sent DESC LIMIT $3", user.ID.String(), after.String(), count)
	} else {
		rows, err = p.Database.Query("SELECT * FROM destination WHERE sender=$1 ORDER BY sent DESC LIMIT $2", user.ID.String(), count)
	}
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
	row := p.Database.QueryRow("SELECT * FROM notifications WHERE id=$1", id.String())
	err := notification.fromRow(row)
	if err == sql.ErrNoRows {
		err = NotificationNotFoundError
	}
	return notification, err
}

func (p *Persister) SendNotificationsToUser(user User, notifications []Notification) ([]Notification, error) {
	stmt := `INSERT INTO notifications VALUE($1, $2, $3, $4, $5, $6, $7, $8, $9);`
	for pos, _ := range notifications {
		id, err := p.GetID()
		if err != nil {
			return []Notification{}, err
		}
		notifications[pos].ID = id
		notifications[pos].Destination = user.ID
		notifications[pos].DestinationType = "user"
		_, err = p.Database.Exec(stmt, notifications[pos].ID.String(), notifications[pos].Destination.String(), notifications[pos].DestinationType, notifications[pos].Nature, notifications[pos].Unread, notifications[pos].ReadBy, notifications[pos].TimeRead, notifications[pos].Sent, notifications[pos].Body)
		if err != nil {
			return []Notification{}, err
		}
	}
	return notifications, nil
}

func (p *Persister) SendNotificationsToDevice(device Device, notifications []Notification) ([]Notification, error) {
	stmt := `INSERT INTO notifications VALUE($1, $2, $3, $4, $5, $6, $7, $8, $9);`
	for pos, _ := range notifications {
		id, err := p.GetID()
		if err != nil {
			return []Notification{}, err
		}
		notifications[pos].ID = id
		notifications[pos].Destination = device.ID
		notifications[pos].DestinationType = "device"
		_, err = p.Database.Exec(stmt, notifications[pos].ID.String(), notifications[pos].Destination.String(), notifications[pos].DestinationType, notifications[pos].Nature, notifications[pos].Unread, notifications[pos].ReadBy, notifications[pos].TimeRead, notifications[pos].Sent, notifications[pos].Body)
		if err != nil {
			return []Notification{}, err
		}
	}
	return notifications, nil
}

func (p *Persister) BroadcastNotifications(notifications []Notification, filter *BroadcastFilter) ([]Notification, error) {
	if filter == nil || !filter.IsValid() {
		return []Notification{}, InvalidBroadcastFilter
	}
	response := []Notification{}
	switch filter.Targets {
	case "users":
		rows, err := p.Database.Query("SELECT * FROM users ORDER BY last_active DESC")
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
		var rows *sql.Rows
		var err error
		if len(filter.ClientType) > 1 {
			queryElems := []string{}
			queryVals := []interface{}{}
			for pos, val := range filter.ClientType {
				queryElems = append(queryElems, "$"+strconv.Itoa(pos))
				queryVals = append(queryVals, val)
			}
			rows, err = p.Database.Query("SELECT * FROM devices WHERE client_type IN("+strings.Join(queryElems, ", ")+") ORDER BY last_seen DESC", queryVals...)
		} else if len(filter.ClientType) == 1 {
			rows, err = p.Database.Query("SELECT * FROM devices WHERE client_type=$1 ORDER BY last_seen DESC", filter.ClientType[0])
		} else {
			rows, err = p.Database.Query("SELECT * FROM devices ORDER BY last_seen DESC")
		}
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
	stmt := `UPDATE notifications SET unread=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, notification.Unread, notification.ID.String())
	return notification, err
}

func (p *Persister) DeleteNotification(notification Notification) error {
	stmt := `DELETE FROM notifications WHERE id=$1;`
	_, err := p.Database.Exec(stmt, notification.ID.String())
	return err
}
