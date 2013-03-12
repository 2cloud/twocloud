package twocloud

import (
	"database/sql"
	"errors"
	"github.com/lib/pq"
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
	rows, err := p.Database.Query("SELECT * FROM devices WHERE user_id=$1 ORDER BY last_seen DESC", user.ID.String())
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
	row := p.Database.QueryRow("SELECT * FROM devices WHERE id=$1", id.String())
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
	stmt := `INSERT INTO devices VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);`
	_, err = p.Database.Exec(stmt, device.ID.String(), device.Name, device.ClientType, device.LastSeen, device.LastIP, device.Created, gcmKey, nil, nil, device.UserID.String())
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
	if gcmKey != nil && clientType != nil && name != nil {
		stmt := `UPDATE devices SET name=$1, client_type=$2, gcm_key=$3 WHERE id=$4;`
		_, err := p.Database.Exec(stmt, device.Name, device.ClientType, device.Pushers.GCM.Key, device.ID.String())
		return err
	} else if gcmKey != nil && clientType != nil {
		stmt := `UPDATE devices SET client_type=$1, gcm_key=$2 WHERE id=$3;`
		_, err := p.Database.Exec(stmt, device.ClientType, device.Pushers.GCM.Key, device.ID.String())
		return err
	} else if gcmKey != nil && name != nil {
		stmt := `UPDATE devices SET name=$1, gcm_key=$2 WHERE id=$3;`
		_, err := p.Database.Exec(stmt, device.Name, device.Pushers.GCM.Key, device.ID.String())
		return err
	} else if name != nil && clientType != nil {
		stmt := `UPDATE devices SET name=$1, client_type=$2, WHERE id=$3;`
		_, err := p.Database.Exec(stmt, device.Name, device.ClientType, device.ID.String())
		return err
	} else if name != nil {
		stmt := `UPDATE devices SET name=$1 WHERE id=$2;`
		_, err := p.Database.Exec(stmt, device.Name, device.ID.String())
		return err
	} else if clientType != nil {
		stmt := `UPDATE devices SET client_type=$1 WHERE id=$2;`
		_, err := p.Database.Exec(stmt, device.ClientType, device.ID.String())
		return err
	} else if gcmKey != nil {
		stmt := `UPDATE devices SET gcm_key=$1 WHERE id=$2;`
		_, err := p.Database.Exec(stmt, device.Pushers.GCM.Key, device.ID.String())
		return err
	}
	return nil
}

func (p *Persister) UpdateDeviceLastSeen(device Device, ip string) (Device, error) {
	now := time.Now()
	device.LastSeen = now
	device.LastIP = ip
	stmt := `UPDATE devices SET last_seen=$1, last_ip=$2 WHERE id=$3;`
	_, err := p.Database.Exec(stmt, device.LastSeen, device.LastIP, device.ID.String())
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
	if pusher == "gcm" {
		if device.Pushers.GCM == nil {
			device.Pushers.GCM = &Pusher{}
		}
		device.Pushers.GCM.LastUsed = now
		stmt := `UPDATE devices SET gcm_last_used=$1 WHERE id=$2;`
		_, err := p.Database.Exec(stmt, device.Pushers.GCM.LastUsed, device.ID.String())
		return err
	} else if pusher == "websockets" {
		if device.Pushers.WebSockets == nil {
			device.Pushers.WebSockets = &Pusher{}
		}
		device.Pushers.WebSockets.LastUsed = now
		stmt := `UPDATE devices SET websockets_last_used=$1 WHERE id=$2;`
		_, err := p.Database.Exec(stmt, device.Pushers.WebSockets.LastUsed, device.ID.String())
		return err
	} else {
		return InvalidPusherType
	}
	return nil
}

func (p *Persister) DeleteDevice(device Device) error {
	stmt := `DELETE FROM devices WHERE id=$1;`
	_, err := p.Database.Exec(stmt, device.ID.String())
	if err != nil {
		return err
	}
	// TODO: cascade deletion to other models
	return nil
}
