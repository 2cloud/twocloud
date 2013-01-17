package twocloud

import (
	"errors"
	"strings"
	"time"
)

type Device struct {
	ID         uint64    `json:"id,omitempty"`
	Name       string    `json:"name,omitempty"`
	LastSeen   time.Time `json:"last_seen,omitempty"`
	LastIP     string    `json:"last_ip,omitempty"`
	ClientType string    `json:"client_type,omitempty"`
	Created    time.Time `json:"created,omitempty"`
	Pushers    *Pushers  `json:"pushers,omitempty"`
	UserID     uint64    `json:"user_id,omitempty"`
}

type Pushers struct {
	GCM        *Pusher `json:"gcm,omitempty"`
	WebSockets *Pusher `json:"websockets,omitempty"`
}

type Pusher struct {
	Key      string    `json:"key,omitempty"`
	LastUsed time.Time `json:"last_used,omitempty"`
}

var InvalidClientType = errors.New("Invalid client type.")
var InvalidPusherType = errors.New("Invalid pusher type.")
var DeviceNotFoundError = errors.New("Device not found.")

var validClientTypes = []string{"android_phone", "android_tablet", "chromebook", "macbook_chrome", "windows_chrome"}

func (d *Device) ValidClientType() bool {
	for _, clientType := range validClientTypes {
		if d.ClientType == clientType {
			return true
		}
	}
	return false
}

// TODO: Query for devices
func (p *Persister) GetDevicesByUser(user User) ([]Device, error) {
	return []Device{}, nil
}

// TODO: Query for device
func (p *Persister) GetDevice(id uint64) (Device, error) {
	return Device{}, nil
}

// TODO: Insert device
func (p *Persister) AddDevice(name, client_type, ip, gcm_key string, user User) (Device, error) {
	id, err := p.GetID()
	if err != nil {
		return Device{}, err
	}
	name = strings.TrimSpace(name)
	client_type = strings.TrimSpace(client_type)
	gcm_key = strings.TrimSpace(gcm_key)
	device := Device{
		ID:         id,
		Name:       name,
		LastSeen:   time.Now(),
		LastIP:     ip,
		ClientType: client_type,
		UserID:     user.ID,
		Created:    time.Now(),
		Pushers: &Pushers{
			GCM: &Pusher{
				Key: gcm_key,
			},
			WebSockets: &Pusher{},
		},
	}
	if !device.ValidClientType() {
		return Device{}, InvalidClientType
	}
	// TODO: persist device
	return device, nil
}

func (p *Persister) UpdateDevice(device Device, name, client_type, gcm_key string) (Device, error) {
	name = strings.TrimSpace(name)
	if name != "" {
		device.Name = name
	}
	client_type = strings.TrimSpace(client_type)
	if client_type != "" {
		device.ClientType = client_type
	}
	gcm_key = strings.TrimSpace(gcm_key)
	if gcm_key != "" {
		if device.Pushers == nil {
			device.Pushers = &Pushers{
				GCM: &Pusher{
					Key: gcm_key,
				},
			}
		} else if device.Pushers.GCM == nil {
			device.Pushers.GCM = &Pusher{
				Key: gcm_key,
			}
		} else {
			device.Pushers.GCM.Key = gcm_key
		}
	}
	if !device.ValidClientType() {
		p.Log.Debug("Invalid client type: %s", device.ClientType)
		return Device{}, InvalidClientType
	}
	// TODO: persist changes
	return device, nil
}

func (p *Persister) UpdateDeviceLastSeen(device Device, ip string) (Device, error) {
	now := time.Now()
	device.LastSeen = now
	device.LastIP = ip
	// TODO: persist changes
	return device, nil
}

func (p *Persister) UpdateDeviceGCMLastUsed(device Device) error {
	return p.updateDevicePusherLastUsed(device, "gcm")
}

func (p *Persister) UpdateDeviceWebSocketLastUsed(device Device) error {
	return p.updateDevicePusherLastUsed(device, "websocket")
}

func (p *Persister) updateDevicePusherLastUsed(device Device, pusher string) error {
	now := time.Now()
	if pusher == "gcm" {
		if device.Pushers != nil && device.Pushers.GCM != nil {
			device.Pushers.GCM.LastUsed = now
			// TODO: persist change
		}
	} else if pusher == "websockets" {
		if device.Pushers != nil && device.Pushers.WebSockets != nil {
			device.Pushers.WebSockets.LastUsed = now
			// TODO: persist change
		}
	} else {
		return InvalidPusherType
	}
	return nil
}

// TODO: delete device
func (p *Persister) DeleteDevice(device Device) error {
	return nil
}
