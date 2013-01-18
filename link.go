package twocloud

import (
	"github.com/PuerkitoBio/purell"
	"time"
)

type URL struct {
	ID          uint64    `json:"id,omitempty"`
	FirstSeen   time.Time `json:"first_seen,omitempty"`
	SentCounter int64     `json:"sent_counter,omitempty"`
	Address     string    `json:"address,omitempty"`
}

type Link struct {
	ID       uint64    `json:"id,omitempty"`
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

// TODO: query for links
func (p *Persister) GetLinksByDevice(device Device, role RoleFlag, before, after uint64, count int) ([]Link, error) {
	return []Link{}, nil
}

// TODO: query for links
func (p *Persister) GetLinksByUser(user User, role RoleFlag, before, after uint64, count int) ([]Link, error) {
	return []Link{}, nil
}

// TODO: query for link
func (p *Persister) GetLink(id uint64) (Link, error) {
	return Link{}, nil
}

// TODO: persist link
func (p *Persister) AddLinks(links []Link) ([]Link, error) {
	urls := []*URL{}
	url_counts := map[uint64]int{}
	for pos, link := range links {
		id, err := p.GetID()
		if err != nil {
			return []Link{}, err
		}
		link.URL.ID = id
		address, err := purell.NormalizeURLString(link.URL.Address, purell.FlagsSafe)
		if err != nil {
			return []Link{}, err
		}
		link.URL.Address = address
		urls = append(urls, link.URL)
		link.URL.FirstSeen = time.Now()
		url_counts[link.URL.ID] = url_counts[link.URL.ID] + 1

		linkID, err := p.GetID()
		if err != nil {
			return []Link{}, err
		}
		links[pos].ID = linkID
		links[pos].Sent = time.Now()
	}
	// TODO: persist/increment urls
	// TODO: persist links
	return links, nil
}

func (p *Persister) AddLink(address, comment string, sender, receiver Device, unread bool) (Link, error) {
	link := Link{
		URL: &URL{
			Address: address,
		},
		Unread:   unread,
		Sender:   sender,
		Receiver: receiver,
		Comment:  comment,
	}
	resp, err := p.AddLinks([]Link{link})
	if err != nil {
		return Link{}, err
	}
	return resp[0], nil
}

// TODO: persist changes
func (p *Persister) UpdateLink(link Link, unread bool, comment string) (Link, error) {
	return Link{}, nil
}

// TODO: delete link
// TODO: decrement url
func (p *Persister) DeleteLink(link Link) error {
	return nil
}
