package twocloud

import (
	"database/sql"
	"github.com/PuerkitoBio/purell"
	"github.com/bmizerany/pq"
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

type Link struct {
	ID       uint64 `json:"id,omitempty"`
	URL      *URL   `json:"url,omitempty"`
	urlID    uint64
	Unread   bool      `json:"unread,omitempty"`
	TimeRead time.Time `json:"time_read,omitempty"`
	Sender   uint64    `json:"sender,omitempty"`
	Receiver uint64    `json:"receiver,omitempty"`
	Comment  string    `json:"comment,omitempty"`
	Sent     time.Time `json:"sent,omitempty"`
}

func (link *Link) fromRow(row ScannableRow) error {
	var comment sql.NullString
	var read pq.NullTime
	link.URL = &URL{}
	err := row.Scan(&link.ID, &link.urlID, &link.Unread, &read, &link.Sender, nil, &link.Receiver, nil, &comment, &link.Sent, &link.URL.ID, &link.URL.Address, &link.URL.SentCounter, &link.URL.FirstSeen)
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
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE (sender=$1 or receiver=$1) and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", device.ID, before, after, count)
		} else if before > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE (sender=$1 or receiver=$1) and id < $2 ORDER BY sent DESC LIMIT $3", device.ID, before, count)
		} else if after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE (sender=$1 or receiver=$1) and id > $2 ORDER BY sent DESC LIMIT $3", device.ID, after, count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender=$1 or receiver=$1 ORDER BY sent DESC LIMIT $2", device.ID, count)
		}
	case RoleSender:
		if before > 0 && after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender=$1 and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", device.ID, before, after, count)
		} else if before > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender=$1 and id < $2 ORDER BY sent DESC LIMIT $3", device.ID, before, count)
		} else if after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender=$1 and id > $2 ORDER BY sent DESC LIMIT $3", device.ID, after, count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender=$1 ORDER BY sent DESC LIMIT $2", device.ID, count)
		}
	case RoleReceiver:
		if before > 0 && after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver=$1 and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", device.ID, before, after, count)
		} else if before > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver=$1 and id < $2 ORDER BY sent DESC LIMIT $3", device.ID, before, count)
		} else if after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver=$1 and id > $2 ORDER BY sent DESC LIMIT $3", device.ID, after, count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver=$1 ORDER BY sent DESC LIMIT $2", device.ID, count)
		}
	}
	if err != nil {
		return []Link{}, err
	}
	for rows.Next() {
		var link Link
		err = link.fromRow(rows)
		if err != nil {
			return []Link{}, err
		}
		links = append(links, link)
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
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE (sender_user=$1 or receiver_user=$1) and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", user.ID, before, after, count)
		} else if before > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE (sender_user=$1 or receiver_user=$1) and id < $2 ORDER BY sent DESC LIMIT $3", user.ID, before, count)
		} else if after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE (sender_user=$1 or receiver_user=$1) and id > $2 ORDER BY sent DESC LIMIT $3", user.ID, after, count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender_user=$1 or receiver_user=$1 ORDER BY sent DESC LIMIT $2", user.ID, count)
		}
	case RoleSender:
		if before > 0 && after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender_user=$1 and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", user.ID, before, after, count)
		} else if before > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender_user=$1 and id < $2 ORDER BY sent DESC LIMIT $3", user.ID, before, count)
		} else if after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender_user=$1 and id > $2 ORDER BY sent DESC LIMIT $3", user.ID, after, count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender_user=$1 ORDER BY sent DESC LIMIT $2", user.ID, count)
		}
	case RoleReceiver:
		if before > 0 && after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver_user=$1 and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", user.ID, before, after, count)
		} else if before > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver_user=$1 and id < $2 ORDER BY sent DESC LIMIT $3", user.ID, before, count)
		} else if after > 0 {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver_user=$1 and id > $2 ORDER BY sent DESC LIMIT $3", user.ID, after, count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver_user=$1 ORDER BY sent DESC LIMIT $2", user.ID, count)
		}
	}
	if err != nil {
		return []Link{}, err
	}
	for rows.Next() {
		var link Link
		err = link.fromRow(rows)
		if err != nil {
			return []Link{}, err
		}
		links = append(links, link)
	}
	err = rows.Err()
	return links, err
}

func (p *Persister) GetLink(id uint64) (Link, error) {
	var link Link
	row := p.Database.QueryRow("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE id=$1", id)
	err := link.fromRow(row)
	return link, err
}

func (p *Persister) AddLinks(links []Link) ([]Link, error) {
	urls := map[uint64]*URL{}
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
		link.URL.FirstSeen = time.Now()
		if _, ok := urls[link.URL.ID]; !ok {
			urls[link.URL.ID] = link.URL
			url_counts[link.URL.ID] = 1
		} else {
			url_counts[link.URL.ID] = url_counts[link.URL.ID] + 1
		}

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
		Sender:   sender.ID,
		Receiver: receiver.ID,
		Comment:  comment,
	}
	resp, err := p.AddLinks([]Link{link})
	if err != nil {
		return Link{}, err
	}
	return resp[0], nil
}

func (p *Persister) UpdateLink(link Link, unread bool, comment string) (Link, error) {
	if comment == "" {
		link.Unread = unread
		link.Comment = comment
		stmt := `UPDATE links SET comment=$1 and unread=$2 WHERE id=$3;`
		_, err := p.Database.Exec(stmt, link.Comment, link.Unread, link.ID)
		return link, err
	}
	link.Unread = unread
	stmt := `UPDATE links SET unread=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, link.Unread, link.ID)
	return link, err
}

func (p *Persister) DeleteLink(link Link) error {
	stmt := `DELETE FROM links WHERE id=$1;`
	_, err := p.Database.Exec(stmt, link.ID)
	if err != nil {
		return err
	}
	// TODO: decrement URL
	return nil
}
