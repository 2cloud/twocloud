package twocloud

import (
	"github.com/PuerkitoBio/purell"
	"github.com/fzzbt/radix/redis"
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
	Unread   bool      `json:"unread,omitempty"`
	TimeRead time.Time `json:"time_read,omitempty"`
	Sender   Device    `json:"sender,omitempty"`
	Receiver Device    `json:"receiver,omitempty"`
	Comment  string    `json:"comment,omitempty"`
	Sent     time.Time `json:"sent,omitempty"`
}

type RoleFlag int

const (
	RoleEither = RoleFlag(iota)
	RoleSender
	RoleReceiver
)

func (r *RequestBundle) GetLinksByDevice(device Device, role RoleFlag, before, after ruid.RUID, count int) ([]Link, error) {
	return []Link{}, nil
}

func (r *RequestBundle) GetLinksByUser(user User, role RoleFlag, before, after ruid.RUID, count int) ([]Link, error) {
	return []Link{}, nil
}

func (r *RequestBundle) GetLink(id ruid.RUID) (Link, error) {
	return Link{}, nil
}

func (r *RequestBundle) AddLinks(links []Link) ([]Link, error) {
	urls := []*URL{}
	url_counts := map[ruid.RUID]int{}
	reservedAddress := []string{}
	for pos, link := range links {
		id, err := gen.Generate([]byte(link.URL.Address))
		if err != nil {
			r.Log.Error(err.Error())
			for _, a := range reservedAddress {
				newErr := r.releaseAddress(a)
				if newErr != nil {
					r.Log.Error(newErr.Error())
				}
			}
			return []Link{}, err
		}
		success, err := r.reserveAddress(link.URL.Address, id)
		if err != nil {
			r.Log.Error(err.Error())
			for _, a := range reservedAddress {
				newErr := r.releaseAddress(a)
				if newErr != nil {
					r.Log.Error(newErr.Error())
				}
			}
			return []Link{}, err
		}
		link.URL.ID = id
		if success {
			reservedAddress = append(reservedAddress, link.URL.Address)
			urls = append(urls, link.URL)
			link.URL.FirstSeen = time.Now()
		} else {
			newID, err := r.getIDFromAddress(link.URL.Address)
			if err != nil {
				r.Log.Error(err.Error())
				for _, a := range reservedAddress {
					newErr := r.releaseAddress(a)
					if newErr != nil {
						r.Log.Error(newErr.Error())
					}
				}
				return []Link{}, err
			}
			link.URL.ID = newID
		}
		url_counts[link.URL.ID] = url_counts[link.URL.ID] + 1
		linkID, err := gen.Generate([]byte(link.URL.Address))
		if err != nil {
			r.Log.Error(err.Error())
			for _, a := range reservedAddress {
				newErr := r.releaseAddress(a)
				if newErr != nil {
					r.Log.Error(newErr.Error())
				}
			}
			return []Link{}, err
		}
		links[pos].ID = linkID
		links[pos].Sent = time.Now()
	}
	err := r.storeURLs(urls)
	if err != nil {
		r.Log.Error(err.Error())
		for _, a := range reservedAddress {
			newErr := r.releaseAddress(a)
			if newErr != nil {
				r.Log.Error(newErr.Error())
			}
		}
		return []Link{}, err
	}
	err = r.storeLinks(links, false)
	if err != nil {
		r.Log.Error(err.Error())
		for _, a := range reservedAddress {
			newErr := r.releaseAddress(a)
			if newErr != nil {
				r.Log.Error(newErr.Error())
			}
		}
		return []Link{}, err
	}
	for url_id, count := range url_counts {
		r.Log.Debug("Incrementing %s by %d", url_id, count)
		err := r.incrementURL(url_id, count)
		if err != nil {
			r.Log.Error(err.Error())
			r.Log.Error("Error incrementing %s by %d", url_id, count)
		}
	}
	return links, nil
}

func (r *RequestBundle) AddLink(address, comment string, sender, receiver Device, unread bool) (Link, error) {
	link := Link{
		URL: &URL{
			Address: address,
		},
		Unread:   unread,
		Sender:   sender,
		Receiver: receiver,
		Comment:  comment,
	}
	resp, err := r.AddLinks([]Link{link})
	if err != nil {
		r.Log.Error(err.Error())
		return Link{}, err
	}
	return resp[0], nil
}

func (r *RequestBundle) storeURLs(urls []*URL) error {
	auditlog := map[ruid.RUID]map[string]interface{}{}
	reply := r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		for _, url := range urls {
			if url == nil {
				continue
			}
			changes := map[string]interface{}{
				"first_seen":   url.FirstSeen.Format(time.RFC3339),
				"sent_counter": 0,
				"address":      url.Address,
			}
			mc.Hmset("urls:"+url.ID.String(), changes)
			auditlog[url.ID] = changes
		}
	})
	// add repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return reply.Err
	}
	from := map[string]interface{}{
		"first_seen": "",
		"sent_counter": "",
		"address": "",
	}
	for id, audit := range auditlog {
		r.AuditMap("urls:"+id.String(), audit, from)
	}
	// add repo calls to instrumentation
	// stop instrumentation
	return nil
}

func (r *RequestBundle) storeLinks(links []Link, update bool) error {
	// start instrumentation
	if update {
		changes := map[ruid.RUID]map[string]interface{}{}
		from := map[ruid.RUID]map[string]interface{}{}
		linksFromID := map[ruid.RUID]Link{}
		reply := r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
			for _, link := range links {
				mc.Hgetall("links:" + link.ID.String())
				linksFromID[link.ID] = link
			}
		})
		// add repo call to instrumentation
		if reply.Err != nil {
			r.Log.Error(reply.Err.Error())
			return reply.Err
		}
		for pos, link := range links {
			if pos > len(reply.Elems) {
				continue
			}
			rep := reply.Elems[pos]
			if rep.Type == redis.ReplyNil {
				continue
			}
			hash, err := rep.Hash()
			if err != nil {
				r.Log.Error(err.Error())
				continue
			}
			time_read, err := time.Parse(time.RFC3339, hash["time_read"])
			if err != nil {
				r.Log.Error(err.Error())
				continue
			}
			if link.Unread != (hash["unread"] == "1") {
				changes[link.ID]["unread"] = link.Unread
				from[link.ID]["unread"] = hash["unread"] == "1"
				changes[link.ID]["time_read"] = time.Now().Format(time.RFC3339)
				from[link.ID]["time_read"] = time_read.Format(time.RFC3339)
			}
			if link.Comment != hash["comment"] {
				changes[link.ID]["comment"] = link.Comment
				from[link.ID]["comment"] = hash["comment"]
			}
		}
		reply = r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
			for id, values := range changes {
				mc.Hmset("links:"+id.String(), values)
				if unread, set := values["unread"]; set && !unread.(bool) {
					mc.Lrem("devices:"+linksFromID[id].Receiver.ID.String()+":links:unread", 0, id.String())
				}
			}
		})
		// add repo call to instrumentation
		if reply.Err != nil {
			r.Log.Error(reply.Err.Error())
			return reply.Err
		}
		for id, _ := range changes {
			r.AuditMap("links:"+id.String(), from[id], changes[id])
		}
		// add repo calls to instrumentation
		return nil
	}
	changes := map[ruid.RUID]map[string]interface{}{}
	senders := map[ruid.RUID][]string{}
	receivers := map[ruid.RUID][]string{}
	unread := map[ruid.RUID][]string{}
	deviceIDs := map[ruid.RUID]string{}
	requestOrder := []ruid.RUID{}
	reply := r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		for _, link := range links {
			values := map[string]interface{}{
				"unread":    link.Unread,
				"time_read": link.TimeRead.Format(time.RFC3339),
				"sender":    link.Sender.ID.String(),
				"receiver":  link.Receiver.ID.String(),
				"comment":   link.Comment,
				"sent":      link.Sent.Format(time.RFC3339),
			}
			if link.URL != nil {
				values["url"] = link.URL.ID.String()
			}
			changes[link.ID] = values
			mc.Hmset("links:"+link.ID.String(), values)
			senders[link.Sender.ID] = append(senders[link.Sender.ID], link.ID.String())
			receivers[link.Receiver.ID] = append(receivers[link.Receiver.ID], link.ID.String())
			if link.Unread {
				unread[link.Receiver.ID] = append(unread[link.Receiver.ID], link.ID.String())
			}
			deviceIDs[link.Sender.ID] = ""
			deviceIDs[link.Receiver.ID] = ""
		}
	})
	// add repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return reply.Err
	}
	reply = r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		for id, _ := range deviceIDs {
			mc.Hget("devices:"+id.String(), "user_id")
			requestOrder = append(requestOrder, id)
		}
	})
	// add repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return reply.Err
	}
	for pos, el := range reply.Elems {
		user_id, err := el.Str()
		if err != nil {
			r.Log.Error(reply.Err.Error())
			continue
		}
		deviceIDs[requestOrder[pos]] = user_id
	}
	reply = r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		for deviceID, linkIDs := range senders {
			mc.Lpush("devices:"+deviceID.String()+":links:sent", linkIDs)
			mc.Lpush("users:"+deviceIDs[deviceID]+":links:sent", linkIDs)
		}
		for deviceID, linkIDs := range unread {
			mc.Lpush("devices:"+deviceID.String()+":links:unread", linkIDs)
			mc.Lpush("users:"+deviceIDs[deviceID]+":links:unread", linkIDs)
		}
		for deviceID, linkIDs := range receivers {
			mc.Lpush("devices:"+deviceID.String()+":links:received", linkIDs)
			mc.Lpush("users:"+deviceIDs[deviceID]+":links:received", linkIDs)
		}
	})
	// add repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return reply.Err
	}
	from := map[string]interface{}{
		"unread":    "",
		"time_read": "",
		"sender":    "",
		"receiver":  "",
		"comment":   "",
		"sent":      "",
		"url":       "",
	}
	for id, _ := range changes {
		r.AuditMap("links:"+id.String(), from, changes[id])
	}
	// add repo calls to instrumentation
	return nil
}

func (r *RequestBundle) getIDFromAddress(address string) (ruid.RUID, error) {
	// start instrumentation
	var err error
	address, err = purell.NormalizeURLString(address, purell.FlagsSafe)
	if err != nil {
		r.Log.Error(err.Error())
		return ruid.RUID(0), err
	}
	reply := r.Repo.client.Hget("urls_to_ids", address)
	// report repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return ruid.RUID(0), reply.Err
	}
	idstr, err := reply.Str()
	if err != nil {
		r.Log.Error(err.Error())
		return ruid.RUID(0), err
	}
	id, err := ruid.RUIDFromString(idstr)
	if err != nil {
		r.Log.Error(err.Error())
		return ruid.RUID(0), err
	}
	return id, nil
	// stop instrumentation
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

func (r *RequestBundle) incrementURL(id ruid.RUID, count int) error {
	r.Log.Debug("About to increment urls:%s by %d", id.String(), count)
	reply := r.Repo.client.Hincrby("urls:"+id.String(), "sent_counter", count)
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return reply.Err
	}
	return nil
}

func (r *RequestBundle) UpdateLink(link Link, unread bool, comment string) (Link, error) {
	return Link{}, nil
}

func (r *RequestBundle) DeleteLink(link Link) error {
	return nil
}
