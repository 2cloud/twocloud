package twocloud

import (
	"github.com/fzzbt/radix/redis"
	"github.com/PuerkitoBio/purell"
	"secondbit.org/ruid"
	"time"
)

type URL struct {
	ID          ruid.RUID `json:"id,omitempty"`
	FirstSeen   time.Time `json:"first_seen,omitempty"`
	SentCounter int64     `json:"sent_counter,omitempty"`
	Address     string    `json:"address,omitempty"`
}

type Link struct {
	ID       ruid.RUID `json:"id,omitempty"`
	URL      *URL      `json:"url,omitempty"`
	Unread   bool      `json:"read,omitempty"`
	TimeRead time.Time `json:"time_read,omitempty"`
	Sender   Device    `json:"sender,omitempty"`
	Receiver Device    `json:"receiver,omitempty"`
	Comment  string    `json:"comment,omitempty"`
	Sent     time.Time `json:"sent,omitempty"`
}

func (r *RequestBundle) GetLinksByDevice(device Device, sender bool) ([]Link, error) {
	return []Link{}, nil
}

func (r *RequestBundle) GetLinksByUser(user User, sender bool) ([]Link, error) {
	return []Link{}, nil
}

func (r *RequestBundle) GetLink(id ruid.RUID) (Link, error) {
	return Link{}, nil
}

func (r *RequestBundle) AddLinks(links []Link) ([]Link, error) {
	return []Link{}, nil
}

func (r *RequestBundle) AddLink(address, comment string, sender, receiver Device, unread bool) (Link, error) {
	link := Link {
		URL: &URL {
			Address: address,
		},
		Unread: unread,
		Sender: sender,
		Receiver: receiver,
		Comment: comment,
	}
	resp, err := r.AddLinks([]Link{link})
	if err != nil {
		r.Log.Error(err.Error())
		return Link{}, err
	}
	if len(resp) < 1 {
		// TODO: Return an error
	}
	return resp[0], nil
}

func (r *RequestBundle) reserveAddress(address string, id ruid.RUID) (bool, error) {
	// start instrumentation
	var err error
	address, err = purell.NormalizeURLString(address, purell.FlagsSafe)
	if err != nil {
		r.Log.Error(err.Error())
		return false, err
	}
	reply := r.Repo.client.Hsetnx("urls_to_ids", address, id.String())
	// report repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return false, reply.Err
	}
	r.Audit("urls_to_ids", address, "", id.String())
	// report repo calls to instrumentation
	// stop instrumentation
	return reply.Bool()
}

func (r *RequestBundle) releaseAddress(address string) error {
	// start instrumentation
	var err error
	address, err = purell.NormalizeURLString(address, purell.FlagsSafe)
	if err != nil {
		r.Log.Error(err.Error())
		return err
	}
	reply := r.Repo.client.Hget("urls_to_ids", address)
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
	reply = r.Repo.client.Hdel("urls_to_ids", address)
	// report the repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(err.Error())
		return reply.Err
	}
	r.Audit("urls_to_ids", address, was, "")
	// report repo calls to instrumentation
	// stop instrumentation
	return nil
}

func (r *RequestBundle) UpdateURL(count int) error {
	return nil
}

func (r *RequestBundle) updateLink(link Link, unread bool, comment string) (Link, error) {
	return Link{}, nil
}

func (r *RequestBundle) DeleteLink(link Link) error {
	return nil
}
