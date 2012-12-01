package twocloud

import (
	"errors"
	"github.com/fzzbt/radix/redis"
	"secondbit.org/ruid"
	"strings"
	"time"
)

type Device struct {
	ID         ruid.RUID `json:"id,omitempty"`
	Name       string    `json:"name,omitempty"`
	LastSeen   time.Time `json:"last_seen,omitempty"`
	LastIP     string    `json:"last_ip,omitempty"`
	ClientType string    `json:"client_type,omitempty"`
	Created    time.Time `json:"created,omitempty"`
	Pushers    *Pushers  `json:"pushers,omitempty"`
	UserID     ruid.RUID `json:"user_id,omitempty"`
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
var DeviceNotFoundError = errors.New("Device not found.")

func (d *Device) ValidClientType() bool {
	return d.ClientType == "android_phone" || d.ClientType == "android_tablet" || d.ClientType == "website" || d.ClientType == "chrome_extension"
}

func (r *RequestBundle) GetDevicesByUser(user User) ([]Device, error) {
	// start instrumentation
	reply := r.Repo.client.Zrevrange("users:"+user.ID.String()+":devices", 0, -1)
	// add repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return []Device{}, reply.Err
	}
	if reply.Type == redis.ReplyNil {
		return []Device{}, nil
	}
	ids, err := reply.List()
	if err != nil {
		return []Device{}, err
	}
	reply = r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		for _, id := range ids {
			mc.Hgetall("devices:" + id)
		}
	})
	// add repo calls to instrumentation
	if reply.Err != nil {
		return []Device{}, reply.Err
	}
	devices := []Device{}
	for pos, rep := range reply.Elems {
		if rep.Type == redis.ReplyNil {
			continue
		}
		hash, err := rep.Hash()
		if err != nil {
			return devices, err
		}
		last_seen, err := time.Parse(time.RFC3339, hash["last_seen"])
		if err != nil {
			return devices, err
		}
		created, err := time.Parse(time.RFC3339, hash["created"])
		if err != nil {
			return devices, err
		}
		user_id, err := ruid.RUIDFromString(hash["user_id"])
		if err != nil {
			return devices, err
		}
		id, err := ruid.RUIDFromString(ids[pos])
		if err != nil {
			return devices, err
		}
		device := Device{
			ID:         id,
			Name:       hash["name"],
			LastSeen:   last_seen,
			LastIP:     hash["last_ip"],
			ClientType: hash["client_type"],
			UserID:     user_id,
			Created:    created,
			Pushers:    &Pushers{},
		}
		if _, exists := hash["gcm_key"]; exists {
			device.Pushers.GCM = &Pusher{
				Key: hash["gcm_key"],
			}
			if _, exists = hash["gcm_last_used"]; exists {
				device.Pushers.GCM.LastUsed, err = time.Parse(time.RFC3339, hash["gcm_last_used"])
				if err != nil {
					r.Log.Error(err.Error())
					return devices, err
				}
			}
		}
		if _, exists := hash["websockets_last_used"]; exists {
			last_used, err := time.Parse(time.RFC3339, hash["websockets_last_used"])
			if err != nil {
				r.Log.Error(err.Error())
				return devices, err
			}
			device.Pushers.WebSockets = &Pusher{
				LastUsed: last_used,
			}
		}
		devices = append(devices, device)
	}
	// stop instrumentation
	return devices, nil
}

func (r *RequestBundle) GetDevice(id ruid.RUID) (Device, error) {
	// start instrumentation
	if r.Device.ID == id {
		return r.Device, nil
	}
	reply := r.Repo.client.Hgetall("devices:" + id.String())
	// add repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return Device{}, reply.Err
	}
	if reply.Type == redis.ReplyNil {
		return Device{}, DeviceNotFoundError
	}
	hash, err := reply.Hash()
	if err != nil {
		r.Log.Error(err.Error())
		return Device{}, err
	}
	last_seen, err := time.Parse(time.RFC3339, hash["last_seen"])
	if err != nil {
		r.Log.Error(err.Error())
		return Device{}, err
	}
	created, err := time.Parse(time.RFC3339, hash["created"])
	if err != nil {
		r.Log.Error(err.Error())
		return Device{}, err
	}
	user_id, err := ruid.RUIDFromString(hash["user_id"])
	if err != nil {
		r.Log.Error(err.Error())
		return Device{}, err
	}
	device := Device{
		ID:         id,
		Name:       hash["name"],
		LastSeen:   last_seen,
		LastIP:     hash["last_ip"],
		ClientType: hash["client_type"],
		UserID:     user_id,
		Created:    created,
		Pushers:    &Pushers{},
	}
	if _, exists := hash["gcm_key"]; exists {
		device.Pushers.GCM = &Pusher{
			Key: hash["gcm_key"],
		}
		if _, exists = hash["gcm_last_used"]; exists {
			device.Pushers.GCM.LastUsed, err = time.Parse(time.RFC3339, hash["gcm_last_used"])
			if err != nil {
				r.Log.Error(err.Error())
				return Device{}, err
			}
		}
	}
	if _, exists := hash["websockets_last_used"]; exists {
		last_used, err := time.Parse(time.RFC3339, hash["websockets_last_used"])
		if err != nil {
			r.Log.Error(err.Error())
			return Device{}, err
		}
		device.Pushers.WebSockets = &Pusher{
			LastUsed: last_used,
		}
	}
	// stop instrumentation
	return device, nil
}

func (r *RequestBundle) AddDevice(name, client_type, ip, gcm_key string, user User) (Device, error) {
	id, err := gen.Generate([]byte(user.ID.String()))
	if err != nil {
		r.Log.Error(err.Error())
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
	err = r.storeDevice(device, false)
	// add repo calls to instrumentation
	if err != nil {
		r.Log.Error(err.Error())
		return Device{}, err
	}
	// log the device creation in stats
	// add repo calls to instrumentation
	// stop instrumentation
	return device, nil
}

func (r *RequestBundle) storeDevice(device Device, update bool) error {
	// start instrumentation
	if update {
		changes := map[string]interface{}{}
		from := map[string]interface{}{}
		old_device := r.Device
		var err error
		if r.Device.ID != device.ID {
			old_device, err = r.GetDevice(device.ID)
			// add repo call to instrumentation
			if err != nil {
				return err
			}
		}
		if old_device.Name != device.Name {
			changes["name"] = device.Name
			from["name"] = old_device.Name
		}
		if old_device.ClientType != device.ClientType {
			changes["client_type"] = device.ClientType
			from["client_type"] = old_device.ClientType
		}
		if old_device.Pushers != nil && old_device.Pushers.GCM != nil && device.Pushers != nil && device.Pushers.GCM != nil && old_device.Pushers.GCM.Key != device.Pushers.GCM.Key {
			changes["gcm_key"] = device.Pushers.GCM.Key
			from["gcm_key"] = old_device.Pushers.GCM.Key
		}
		reply := r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
			mc.Hmset("devices:"+device.ID.String(), changes)
		})
		// add repo call to instrumentation
		if reply.Err != nil {
			r.Log.Error(reply.Err.Error())
			return reply.Err
		}
		r.AuditMap("devices:"+device.ID.String(), from, changes)
		// add repo call to instrumentation
		return nil
	}
	changes := map[string]interface{}{
		"name":        device.Name,
		"last_seen":   time.Now().Format(time.RFC3339),
		"last_ip":     device.LastIP,
		"client_type": device.ClientType,
		"created":     time.Now().Format(time.RFC3339),
		"user_id":     device.UserID.String(),
	}
	from := map[string]interface{}{
		"name":        "",
		"last_seen":   "",
		"last_ip":     "",
		"client_type": "",
		"created":     "",
		"user_id":     "",
	}
	if device.Pushers != nil && device.Pushers.GCM != nil {
		changes["gcm_key"] = device.Pushers.GCM.Key
		from["gcm_key"] = ""
	}
	reply := r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		mc.Hmset("devices:"+device.ID.String(), changes)
		mc.Zadd("users:"+device.UserID.String()+":devices", device.LastSeen.Unix(), device.ID.String())
	})
	// add repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return reply.Err
	}
	r.AuditMap("devices:"+device.ID.String(), from, changes)
	// add repo call to instrumentation
	// stop instrumentation
	return nil
}

func (r *RequestBundle) UpdateDevice(device Device, name, client_type, gcm_key string) (Device, error) {
	return Device{}, nil
}

func (r *RequestBundle) UpdateDeviceLastSeen(device Device, ip string) (Device, error) {
	now := time.Now()
	reply := r.Repo.client.Hmset("devices:"+device.ID.String(), "last_seen", now.Format(time.RFC3339), "last_ip", ip)
	// add repo call to instrumentation
	if reply.Err != nil {
		return Device{}, reply.Err
	}
	from := map[string]interface{}{
		"last_seen": device.LastSeen.Format(time.RFC3339),
		"last_ip":   device.LastIP,
	}
	device.LastSeen = now
	device.LastIP = ip
	to := map[string]interface{}{
		"last_seen": now.Format(time.RFC3339),
		"last_ip":   ip,
	}
	r.AuditMap("devices:"+device.ID.String(), from, to)
	// add repo call to instrumentation
	// stop instrumentation
	return device, nil
}

func (r *RequestBundle) UpdateDeviceGCMLastUsed(device Device) error {
	return r.updateDevicePusherLastUsed(device, "gcm")
}

func (r *RequestBundle) UpdateDeviceWebSocketLastUsed(device Device) error {
	return r.updateDevicePusherLastUsed(device, "websocket")
}

func (r *RequestBundle) updateDevicePusherLastUsed(device Device, pusher string) error {
	if pusher == "gcm" {
		// TODO: update gcm last used
	} else if pusher == "websocket" {
		// TODO: update websocket last used
	}
	return nil
}

func (r *RequestBundle) DeleteDevice(device Device) error {
	return nil
}
