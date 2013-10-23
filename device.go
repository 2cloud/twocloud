package twocloud

import (
	"database/sql"
	"errors"
	"github.com/lib/pq"
	"secondbit.org/pan"
	"strings"
	"time"
)

var DeviceTableCreateStatement = `CREATE TABLE devices (
	id varchar primary key,
	name varchar NOT NULL,
	client_type varchar NOT NULL,
	last_seen timestamp default CURRENT_TIMESTAMP,
	last_ip varchar,
	created timestamp default CURRENT_TIMESTAMP,
	gcm_key varchar,
	gcm_last_used timestamp,
	websockets_last_used timestamp,
	user_id varchar NOT NULL);`

type Device struct {
	ID         ID        `json:"id,omitempty"`
	Name       string    `json:"name,omitempty"`
	LastSeen   time.Time `json:"last_seen,omitempty"`
	LastIP     string    `json:"last_ip,omitempty"`
	ClientType string    `json:"client_type,omitempty"`
	Created    time.Time `json:"created,omitempty"`
	Pushers    *Pushers  `json:"pushers,omitempty"`
	UserID     ID        `json:"user_id,omitempty"`
}

type Pushers struct {
	GCM        *Pusher `json:"gcm,omitempty"`
	WebSockets *Pusher `json:"websockets,omitempty"`
}

type Pusher struct {
	Key      string    `json:"key,omitempty"`
	LastUsed time.Time `json:"last_used,omitempty"`
}

func (device *Device) fromRow(row ScannableRow) error {
	var gcm_key sql.NullString
	var gcm_last_used, websockets_last_used pq.NullTime
	var idStr, userIDStr string
	err := row.Scan(&idStr, &device.Name, &device.ClientType, &device.LastSeen, &device.LastIP, &device.Created, &gcm_key, &gcm_last_used, &websockets_last_used, &userIDStr)
	if err != nil {
		return err
	}
	id, err := IDFromString(idStr)
	if err != nil {
		return err
	}
	device.ID = id
	userID, err := IDFromString(userIDStr)
	if err != nil {
		return err
	}
	device.UserID = userID
	if gcm_key.Valid {
		if device.Pushers == nil {
			device.Pushers = &Pushers{
				GCM: &Pusher{
					Key: gcm_key.String,
				},
			}
		} else if device.Pushers.GCM == nil {
			device.Pushers.GCM = &Pusher{
				Key: gcm_key.String,
			}
		} else {
			device.Pushers.GCM.Key = gcm_key.String
		}
	}
	if gcm_last_used.Valid {
		if device.Pushers == nil {
			device.Pushers = &Pushers{
				GCM: &Pusher{
					LastUsed: gcm_last_used.Time,
				},
			}
		} else if device.Pushers.GCM == nil {
			device.Pushers.GCM = &Pusher{
				LastUsed: gcm_last_used.Time,
			}
		} else {
			device.Pushers.GCM.LastUsed = gcm_last_used.Time
		}
	}
	if websockets_last_used.Valid {
		if device.Pushers == nil {
			device.Pushers = &Pushers{
				WebSockets: &Pusher{
					LastUsed: websockets_last_used.Time,
				},
			}
		} else if device.Pushers.WebSockets == nil {
			device.Pushers.WebSockets = &Pusher{
				LastUsed: websockets_last_used.Time,
			}
		} else {
			device.Pushers.WebSockets.LastUsed = websockets_last_used.Time
		}
	}
	return nil
}

var InvalidClientType = errors.New("Invalid client type.")
var InvalidPusherType = errors.New("Invalid pusher type.")
var DeviceNotFoundError = errors.New("Device not found.")

var validClientTypes = []string{"android_phone", "android_tablet", "android_tablet_small", "chromebook", "macbook_chrome", "windows_chrome"}

func (d *Device) ValidClientType() bool {
	for _, clientType := range validClientTypes {
		if d.ClientType == clientType {
			return true
		}
	}
	return false
}

func (p *Persister) GetDevicesByUser(user User) ([]Device, error) {
	devices := []Device{}
	query := pan.New()
	query.SQL = "SELECT * FROM devices"
	query.IncludeWhere()
	query.Include("user_id=?", user.ID.String())
	query.IncludeOrder("last_seen DESC")
	rows, err := p.Database.Query(query.Generate(" "), query.Args...)
	if err != nil {
		return []Device{}, err
	}
	for rows.Next() {
		var device Device
		err = device.fromRow(rows)
		if err != nil {
			return []Device{}, err
		}
		devices = append(devices, device)
	}
	err = rows.Err()
	return devices, err
}

func (p *Persister) GetDevice(id ID) (Device, error) {
	var device Device
	query := pan.New()
	query.SQL = "SELECT * FROM devices"
	query.IncludeWhere()
	query.Include("id=?", id.String())
	row := p.Database.QueryRow(query.Generate(" "), query.Args...)
	err := device.fromRow(row)
	if err == sql.ErrNoRows {
		err = DeviceNotFoundError
	}
	return device, err
}

func (p *Persister) AddDevice(name, clientType, ip string, gcmKey *string, user User) (Device, error) {
	id, err := p.GetID()
	if err != nil {
		return Device{}, err
	}
	name = strings.TrimSpace(name)
	clientType = strings.TrimSpace(clientType)
	device := Device{
		ID:         id,
		Name:       name,
		LastSeen:   time.Now(),
		LastIP:     ip,
		ClientType: clientType,
		UserID:     user.ID,
		Created:    time.Now(),
	}
	if gcmKey != nil {
		device.Pushers = &Pushers{
			GCM: &Pusher{
				Key: strings.TrimSpace(*gcmKey),
			},
		}
	}
	if !device.ValidClientType() {
		return Device{}, InvalidClientType
	}
	query := pan.New()
	query.SQL = "INSERT INTO devices VALUES("
	query.Include("?", device.ID.String())
	query.Include("?", device.Name)
	query.Include("?", device.ClientType)
	query.Include("?", device.LastSeen)
	query.Include("?", device.LastIP)
	query.Include("?", device.Created)
	query.Include("?", gcmKey)
	query.Include("?", nil)
	query.Include("?", nil)
	query.Include("?", device.UserID.String())
	query.FlushExpressions(", ")
	query.SQL += ")"
	_, err = p.Database.Exec(query.Generate(" "), query.Args...)
	return device, err
}

func (p *Persister) UpdateDevice(device *Device, name, clientType, gcmKey *string) error {
	if name != nil {
		device.Name = strings.TrimSpace(*name)
		name = &device.Name
	}
	if clientType != nil {
		device.ClientType = strings.TrimSpace(strings.ToLower(*clientType))
		clientType = &device.ClientType
	}
	if gcmKey != nil {
		if device.Pushers == nil {
			device.Pushers = &Pushers{
				GCM: &Pusher{
					Key: strings.TrimSpace(*gcmKey),
				},
			}
		} else if device.Pushers.GCM == nil {
			device.Pushers.GCM = &Pusher{
				Key: strings.TrimSpace(*gcmKey),
			}
		} else {
			device.Pushers.GCM.Key = strings.TrimSpace(*gcmKey)
		}
		gcmKey = &device.Pushers.GCM.Key
	}
	if !device.ValidClientType() {
		return InvalidClientType
	}
	query := pan.New()
	query.SQL = "UPDATE devices SET"
	query.IncludeIfNotNil("gcm_key=?", gcmKey)
	query.IncludeIfNotNil("client_type=?", clientType)
	query.IncludeIfNotNil("name=?", device.Name)
	query.FlushExpressions(", ")
	query.IncludeWhere()
	query.Include("id=?", device.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}

func (p *Persister) UpdateDeviceLastSeen(device Device, ip string) (Device, error) {
	now := time.Now()
	device.LastSeen = now
	device.LastIP = ip
	query := pan.New()
	query.SQL = "UPDATE devices SET"
	query.Include("last_seen=?", device.LastSeen)
	query.Include("last_ip=?", device.LastIP)
	query.FlushExpressions(", ")
	query.IncludeWhere()
	query.Include("id=?", device.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return device, err
}

func (p *Persister) UpdateDeviceGCMLastUsed(device Device) error {
	return p.updateDevicePusherLastUsed(device, "gcm")
}

func (p *Persister) UpdateDeviceWebSocketLastUsed(device Device) error {
	return p.updateDevicePusherLastUsed(device, "websocket")
}

func (p *Persister) updateDevicePusherLastUsed(device Device, pusher string) error {
	now := time.Now()
	if device.Pushers == nil {
		device.Pushers = &Pushers{}
	}
	query := pan.New()
	query.SQL = "UPDATE devices SET"
	if pusher == "gcm" {
		if device.Pushers.GCM == nil {
			device.Pushers.GCM = &Pusher{}
		}
		device.Pushers.GCM.LastUsed = now
		query.Include("gcm_last_used=?", device.Pushers.GCM.LastUsed)
	} else if pusher == "websockets" {
		if device.Pushers.WebSockets == nil {
			device.Pushers.WebSockets = &Pusher{}
		}
		device.Pushers.WebSockets.LastUsed = now
		query.Include("websockets_last_used=?", device.Pushers.WebSockets.LastUsed)
	} else {
		return InvalidPusherType
	}
	query.IncludeWhere()
	query.Include("id=?", device.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}

func (p *Persister) DeleteDevice(device Device, cascade bool) error {
	return p.DeleteDevices([]Device{device}, cascade)
}

func (p *Persister) DeleteDevices(devices []Device, cascade bool) error {
	query := pan.New()
	query.SQL = "DELETE FROM devices"
	query.IncludeWhere()
	queryKeys := make([]string, len(devices))
	queryVals := make([]interface{}, len(devices))
	for _, device := range devices {
		queryKeys = append(queryKeys, "?")
		queryVals = append(queryVals, device.ID.String())
	}
	query.Include("id IN("+strings.Join(queryKeys, ", ")+")", queryVals...)
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err != nil {
		return err
	}
	if cascade {
		// Delete links sent to those devices
		err = p.DeleteLinksByDevices(devices)
		if err != nil {
			return err
		}
		// Delete notifications sent to those devices
		err = p.DeleteNotificationsByDevices(devices)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Persister) DeleteDevicesByUsers(users []User, cascade bool) error {
	query := pan.New()
	query.SQL = "DELETE FROM devices"
	query.IncludeWhere()
	queryKeys := make([]string, len(users))
	queryVals := make([]interface{}, len(users))
	for _, user := range users {
		queryKeys = append(queryKeys, "?")
		queryVals = append(queryVals, user.ID.String())
	}
	query.Include("user_id IN("+strings.Join(queryKeys, ", ")+")", queryVals...)
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err != nil {
		return err
	}
	if cascade {
		// Delete links sent to those users
		err = p.DeleteLinksByUsers(users)
		if err != nil {
			return err
		}
		// Delete notifications sent to those users
		err = p.DeleteNotificationsByUsers(users)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Persister) DeleteDevicesByUser(user User, cascade bool) error {
	return p.DeleteDevicesByUsers([]User{user}, cascade)
}
