package twocloud

import (
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

func (d *Device) ValidClientType() bool {
	return d.ClientType == "android_phone" || d.ClientType == "android_tablet" || d.ClientType == "website" || d.ClientType == "chrome_extension"
}

func (r *RequestBundle) GetDevicesByUser(user User) ([]Device, error) {
	return []Device{}, nil
}

func (r *RequestBundle) GetDevice(id ruid.RUID) (Device, error) {
	if r.Device.ID == id {
		return r.Device, nil
	}
	return Device{}, nil
}

func (r *RequestBundle) AddDevice(name, client_type, ip, gcm_key string, user User) (Device, error) {
	return Device{}, nil
}

func (r *RequestBundle) reserveDeviceName(name string, id ruid.RUID) (bool, error) {
	// start instrumentation
	reply := r.Repo.client.Hsetnx("device_names_to_ids", strings.ToLower(name), id.String())
	// report repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return false, reply.Err
	}
	r.Audit("device_names_to_ids", strings.ToLower(name), "", id.String())
	// report repo calls to instrumentation
	// stop instrumentation
	return reply.Bool()
}

func (r *RequestBundle) releaseDeviceName(name string) error {
	// start instrumentation
	reply := r.Repo.client.Hget("device_names_to_ids", strings.ToLower(name))
	// report the repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return reply.Err
	}
	if reply.Type == redis.ReplyNil {
		return nil
	}
	was, err := reply.Str()
	if err != nil {
		r.Log.Error(err.Error())
		return err
	}
	reply = r.Repo.client.Hdel("device_names_to_ids", strings.ToLower(name))
	// report the repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(err.Error())
		return reply.Err
	}
	r.Audit("device_names_to_ids", strings.ToLower(name), was, "")
	// report repo calls to instrumentation
	// stop instrumentation
	return nil
}

func (r *RequestBundle) UpdateDevice(device Device, name, client_type, gcm_key string) (Device, error) {
	return Device {}, nil
}

func (r *RequestBundle) UpdateDeviceLastSeen(device Device, ip string) (Device, error) {
	return Device{}, nil
}

func (r *RequestBundle) DeleteDevice(device Device) error {
	return nil
}
