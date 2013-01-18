package twocloud

import (
	"database/sql"
	"github.com/PuerkitoBio/purell"
	"github.com/bmizerany/pq"
	"strconv"
	"strings"
	"time"
)

var URLTableCreateStatement = `CREATE TABLE urls (
	id bigint primary key,
	address varchar NOT NULL UNIQUE,
	sent_counter bigint default 0,
	first_seen timestamp default CURRENT_TIMESTAMP);`

var LinkTableCreateStatement = `CREATE TABLE links (
	id bigint primary key,
	url bigint NOT NULL,
	unread bool default true,
	time_read timestamp,
	sender bigint NOT NULL,
	sender_user bigint NOT NULL,
	receiver bigint NOT NULL,
	receiver_user bigint NOT NULL,
	comment varchar,
	sent timestamp default CURRENT_TIMESTAMP);`

type URL struct {
	ID          uint64    `json:"id,omitempty"`
	FirstSeen   time.Time `json:"first_seen,omitempty"`
	SentCounter int64     `json:"sent_counter,omitempty"`
	Address     string    `json:"address,omitempty"`
}

func (url *URL) fromRow(row ScannableRow) error {
	return row.Scan(&url.ID, &url.Address, &url.SentCounter, &url.FirstSeen)
}

type Link struct {
	ID         uint64 `json:"id,omitempty"`
	URL        *URL   `json:"url,omitempty"`
	urlID      uint64
	Unread     bool      `json:"unread,omitempty"`
	TimeRead   time.Time `json:"time_read,omitempty"`
	Sender     Device    `json:"sender,omitempty"`
	senderID   uint64
	Receiver   Device `json:"receiver,omitempty"`
	receiverID uint64
	Comment    string    `json:"comment,omitempty"`
	Sent       time.Time `json:"sent,omitempty"`
}

func (link *Link) fromRow(row ScannableRow) error {
	var comment sql.NullString
	var read pq.NullTime
	err := row.Scan(&link.ID, &link.urlID, &link.Unread, &read, &link.senderID, nil, &link.receiverID, nil, &comment, &link.Sent)
	if err != nil {
		return err
	}
	if comment.Valid {
		link.Comment = comment.String
	} else {
		link.Comment = ""
	}
	if read.Valid {
		link.TimeRead = read.Time
	}
	return nil
}

type RoleFlag int

const (
	RoleEither = RoleFlag(iota)
	RoleSender
	RoleReceiver
)

func (p *Persister) GetLinksByDevice(device Device, role RoleFlag, before, after uint64, count int) ([]Link, error) {
	links := []Link{}
	var rows *sql.Rows
	var err error
	switch role {
	case RoleEither:
		if before > 0 && after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE (sender=$1 or receiver=$1) and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", device.ID, before, after, count)
		} else if before > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE (sender=$1 or receiver=$1) and id < $2 ORDER BY sent DESC LIMIT $3", device.ID, before, count)
		} else if after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE (sender=$1 or receiver=$1) and id > $2 ORDER BY sent DESC LIMIT $3", device.ID, after, count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links WHERE sender=$1 or receiver=$1 ORDER BY sent DESC LIMIT $2", device.ID, count)
		}
	case RoleSender:
		if before > 0 && after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE sender=$1 and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", device.ID, before, after, count)
		} else if before > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE sender=$1 and id < $2 ORDER BY sent DESC LIMIT $3", device.ID, before, count)
		} else if after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE sender=$1 and id > $2 ORDER BY sent DESC LIMIT $3", device.ID, after, count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links WHERE sender=$1 ORDER BY sent DESC LIMIT $2", device.ID, count)
		}
	case RoleReceiver:
		if before > 0 && after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE receiver=$1 and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", device.ID, before, after, count)
		} else if before > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE receiver=$1 and id < $2 ORDER BY sent DESC LIMIT $3", device.ID, before, count)
		} else if after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE receiver=$1 and id > $2 ORDER BY sent DESC LIMIT $3", device.ID, after, count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links WHERE receiver=$1 ORDER BY sent DESC LIMIT $2", device.ID, count)
		}
	}
	if err != nil {
		return []Link{}, err
	}
	urlIDs := map[uint64][]int{}
	deviceIDs := map[uint64][]int{}
	for rows.Next() {
		var link Link
		err = link.fromRow(rows)
		if err != nil {
			return []Link{}, err
		}
		if _, ok := urlIDs[link.urlID]; !ok {
			urlIDs[link.urlID] = []int{}
		}
		urlIDs[link.urlID] = append(urlIDs[link.urlID], len(links))
		if _, ok := deviceIDs[link.senderID]; !ok {
			deviceIDs[link.senderID] = []int{}
		}
		deviceIDs[link.senderID] = append(deviceIDs[link.senderID], len(links))
		if _, ok := deviceIDs[link.receiverID]; !ok {
			deviceIDs[link.receiverID] = []int{}
		}
		deviceIDs[link.receiverID] = append(deviceIDs[link.receiverID], len(links))
		links = append(links, link)
	}
	err = rows.Err()
	if err != nil {
		return []Link{}, err
	}
	urlKeys := []string{}
	urlValues := []interface{}{}
	for k, _ := range urlIDs {
		urlValues = append(urlValues, k)
		urlKeys = append(urlKeys, "$"+strconv.Itoa(len(urlValues)))
	}
	rows, err = p.Database.Query("SELECT * FROM urls WHERE id IN ("+strings.Join(urlKeys, ", ")+")", urlValues...)
	if err != nil {
		return []Link{}, err
	}
	for rows.Next() {
		var url *URL
		err = url.fromRow(rows)
		if err != nil {
			return []Link{}, err
		}
		for _, linkPos := range urlIDs[url.ID] {
			links[linkPos].URL = url
		}
	}
	err = rows.Err()
	if err != nil {
		return []Link{}, err
	}
	deviceKeys := []string{}
	deviceValues := []interface{}{}
	for k, _ := range deviceIDs {
		deviceValues = append(deviceValues, k)
		deviceKeys = append(deviceKeys, "$"+strconv.Itoa(len(deviceValues)))
	}
	rows, err = p.Database.Query("SELECT * FROM devices WHERE id IN ("+strings.Join(deviceKeys, ", ")+")", deviceValues...)
	if err != nil {
		return []Link{}, err
	}
	for rows.Next() {
		var device Device
		err = device.fromRow(rows)
		if err != nil {
			return []Link{}, err
		}
		for _, linkPos := range deviceIDs[device.ID] {
			if links[linkPos].senderID == device.ID {
				links[linkPos].Sender = device
			}
			if links[linkPos].receiverID == device.ID {
				links[linkPos].Receiver = device
			}
		}
	}
	err = rows.Err()
	return links, err
}

func (p *Persister) GetLinksByUser(user User, role RoleFlag, before, after uint64, count int) ([]Link, error) {
	links := []Link{}
	var rows *sql.Rows
	var err error
	switch role {
	case RoleEither:
		if before > 0 && after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE (sender_user=$1 or receiver_user=$1) and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", user.ID, before, after, count)
		} else if before > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE (sender_user=$1 or receiver_user=$1) and id < $2 ORDER BY sent DESC LIMIT $3", user.ID, before, count)
		} else if after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE (sender_user=$1 or receiver_user=$1) and id > $2 ORDER BY sent DESC LIMIT $3", user.ID, after, count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links WHERE sender_user=$1 or receiver_user=$1 ORDER BY sent DESC LIMIT $2", user.ID, count)
		}
	case RoleSender:
		if before > 0 && after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE sender_user=$1 and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", user.ID, before, after, count)
		} else if before > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE sender_user=$1 and id < $2 ORDER BY sent DESC LIMIT $3", user.ID, before, count)
		} else if after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE sender_user=$1 and id > $2 ORDER BY sent DESC LIMIT $3", user.ID, after, count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links WHERE sender_user=$1 ORDER BY sent DESC LIMIT $2", user.ID, count)
		}
	case RoleReceiver:
		if before > 0 && after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE receiver_user=$1 and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", user.ID, before, after, count)
		} else if before > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE receiver_user=$1 and id < $2 ORDER BY sent DESC LIMIT $3", user.ID, before, count)
		} else if after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links WHERE receiver_user=$1 and id > $2 ORDER BY sent DESC LIMIT $3", user.ID, after, count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links WHERE receiver_user=$1 ORDER BY sent DESC LIMIT $2", user.ID, count)
		}
	}
	if err != nil {
		return []Link{}, err
	}
	urlIDs := map[uint64][]int{}
	deviceIDs := map[uint64][]int{}
	for rows.Next() {
		var link Link
		err = link.fromRow(rows)
		if err != nil {
			return []Link{}, err
		}
		if _, ok := urlIDs[link.urlID]; !ok {
			urlIDs[link.urlID] = []int{}
		}
		urlIDs[link.urlID] = append(urlIDs[link.urlID], len(links))
		if _, ok := deviceIDs[link.senderID]; !ok {
			deviceIDs[link.senderID] = []int{}
		}
		deviceIDs[link.senderID] = append(deviceIDs[link.senderID], len(links))
		if _, ok := deviceIDs[link.receiverID]; !ok {
			deviceIDs[link.receiverID] = []int{}
		}
		deviceIDs[link.receiverID] = append(deviceIDs[link.receiverID], len(links))
		links = append(links, link)
	}
	err = rows.Err()
	if err != nil {
		return []Link{}, err
	}
	urlKeys := []string{}
	urlValues := []interface{}{}
	for k, _ := range urlIDs {
		urlValues = append(urlValues, k)
		urlKeys = append(urlKeys, "$"+strconv.Itoa(len(urlValues)))
	}
	rows, err = p.Database.Query("SELECT * FROM urls WHERE id IN ("+strings.Join(urlKeys, ", ")+")", urlValues...)
	if err != nil {
		return []Link{}, err
	}
	for rows.Next() {
		var url *URL
		err = url.fromRow(rows)
		if err != nil {
			return []Link{}, err
		}
		for _, linkPos := range urlIDs[url.ID] {
			links[linkPos].URL = url
		}
	}
	err = rows.Err()
	if err != nil {
		return []Link{}, err
	}
	deviceKeys := []string{}
	deviceValues := []interface{}{}
	for k, _ := range deviceIDs {
		deviceValues = append(deviceValues, k)
		deviceKeys = append(deviceKeys, "$"+strconv.Itoa(len(deviceValues)))
	}
	rows, err = p.Database.Query("SELECT * FROM devices WHERE id IN ("+strings.Join(deviceKeys, ", ")+")", deviceValues...)
	if err != nil {
		return []Link{}, err
	}
	for rows.Next() {
		var device Device
		err = device.fromRow(rows)
		if err != nil {
			return []Link{}, err
		}
		for _, linkPos := range deviceIDs[device.ID] {
			if links[linkPos].senderID == device.ID {
				links[linkPos].Sender = device
			}
			if links[linkPos].receiverID == device.ID {
				links[linkPos].Receiver = device
			}
		}
	}
	err = rows.Err()
	return links, err
}

// TODO: query for link
func (p *Persister) GetLink(id uint64) (Link, error) {
	return Link{}, nil
}

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
