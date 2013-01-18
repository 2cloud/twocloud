package twocloud

import (
	"database/sql"
	"errors"
	"github.com/PuerkitoBio/purell"
	"github.com/bmizerany/pq"
	"time"
)

var URLTableCreateStatement = `CREATE TABLE urls (
	id varchar primary key,
	address varchar NOT NULL UNIQUE,
	sent_counter bigint default 0,
	first_seen timestamp default CURRENT_TIMESTAMP);`

var LinkTableCreateStatement = `CREATE TABLE links (
	id varchar primary key,
	url varchar NOT NULL,
	unread bool default true,
	time_read timestamp,
	sender varchar NOT NULL,
	sender_user varchar NOT NULL,
	receiver varchar NOT NULL,
	receiver_user varchar NOT NULL,
	comment varchar,
	sent timestamp default CURRENT_TIMESTAMP);`

type URL struct {
	ID          ID        `json:"id,omitempty"`
	FirstSeen   time.Time `json:"first_seen,omitempty"`
	SentCounter int64     `json:"sent_counter,omitempty"`
	Address     string    `json:"address,omitempty"`
}

func (url *URL) fromRow(row ScannableRow) error {
	var idStr string
	err := row.Scan(&idStr, &url.Address, &url.SentCounter, &url.FirstSeen)
	if err != nil {
		return err
	}
	id, err := IDFromString(idStr)
	if err != nil {
		return err
	}
	url.ID = id
	return nil
}

type Link struct {
	ID           ID        `json:"id,omitempty"`
	URL          *URL      `json:"url,omitempty"`
	Unread       bool      `json:"unread,omitempty"`
	TimeRead     time.Time `json:"time_read,omitempty"`
	Sender       ID        `json:"sender,omitempty"`
	SenderUser   ID        `json:"-"`
	Receiver     ID        `json:"receiver,omitempty"`
	ReceiverUser ID        `json:"-"`
	Comment      string    `json:"comment,omitempty"`
	Sent         time.Time `json:"sent,omitempty"`
}

func (link *Link) fromRow(row ScannableRow) error {
	var comment sql.NullString
	var read pq.NullTime
	var idStr, urlIDStr, senderIDStr, senderUserIDStr, receiverIDStr, receiverUserIDStr string
	link.URL = &URL{}
	err := row.Scan(&idStr, &urlIDStr, &link.Unread, &read, &senderIDStr, &senderUserIDStr, &receiverIDStr, &receiverUserIDStr, &comment, &link.Sent, &urlIDStr, &link.URL.Address, &link.URL.SentCounter, &link.URL.FirstSeen)
	if err != nil {
		return err
	}
	id, err := IDFromString(idStr)
	if err != nil {
		return err
	}
	link.ID = id
	urlID, err := IDFromString(urlIDStr)
	if err != nil {
		return err
	}
	link.URL.ID = urlID
	senderID, err := IDFromString(senderIDStr)
	if err != nil {
		return err
	}
	link.Sender = senderID
	senderUserID, err := IDFromString(senderUserIDStr)
	if err != nil {
		return err
	}
	link.SenderUser = senderUserID
	receiverID, err := IDFromString(receiverIDStr)
	if err != nil {
		return err
	}
	link.Receiver = receiverID
	receiverUserID, err := IDFromString(receiverUserIDStr)
	if err != nil {
		return err
	}
	link.ReceiverUser = receiverUserID
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

var NilURLError = errors.New("Links must supply a URL.")
var LinkNotFoundError = errors.New("Link not found.")

func (p *Persister) GetLinksByDevice(device Device, role RoleFlag, before, after ID, count int) ([]Link, error) {
	links := []Link{}
	var rows *sql.Rows
	var err error
	switch role {
	case RoleEither:
		if !before.IsZero() && !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE (sender=$1 or receiver=$1) and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", device.ID.String(), before.String(), after.String(), count)
		} else if !before.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE (sender=$1 or receiver=$1) and id < $2 ORDER BY sent DESC LIMIT $3", device.ID.String(), before.String(), count)
		} else if !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE (sender=$1 or receiver=$1) and id > $2 ORDER BY sent DESC LIMIT $3", device.ID.String(), after.String(), count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender=$1 or receiver=$1 ORDER BY sent DESC LIMIT $2", device.ID.String(), count)
		}
	case RoleSender:
		if !before.IsZero() && !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender=$1 and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", device.ID.String(), before.String(), after.String(), count)
		} else if !before.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender=$1 and id < $2 ORDER BY sent DESC LIMIT $3", device.ID.String(), before.String(), count)
		} else if !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender=$1 and id > $2 ORDER BY sent DESC LIMIT $3", device.ID.String(), after.String(), count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender=$1 ORDER BY sent DESC LIMIT $2", device.ID.String(), count)
		}
	case RoleReceiver:
		if !before.IsZero() && !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver=$1 and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", device.ID.String(), before.String(), after.String(), count)
		} else if !before.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver=$1 and id < $2 ORDER BY sent DESC LIMIT $3", device.ID.String(), before.String(), count)
		} else if !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver=$1 and id > $2 ORDER BY sent DESC LIMIT $3", device.ID.String(), after.String(), count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver=$1 ORDER BY sent DESC LIMIT $2", device.ID.String(), count)
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

func (p *Persister) GetLinksByUser(user User, role RoleFlag, before, after ID, count int) ([]Link, error) {
	links := []Link{}
	var rows *sql.Rows
	var err error
	switch role {
	case RoleEither:
		if !before.IsZero() && !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE (sender_user=$1 or receiver_user=$1) and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", user.ID.String(), before.String(), after.String(), count)
		} else if !before.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE (sender_user=$1 or receiver_user=$1) and id < $2 ORDER BY sent DESC LIMIT $3", user.ID.String(), before.String(), count)
		} else if !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE (sender_user=$1 or receiver_user=$1) and id > $2 ORDER BY sent DESC LIMIT $3", user.ID.String(), after.String(), count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender_user=$1 or receiver_user=$1 ORDER BY sent DESC LIMIT $2", user.ID.String(), count)
		}
	case RoleSender:
		if !before.IsZero() && !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender_user=$1 and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", user.ID.String(), before.String(), after.String(), count)
		} else if !before.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender_user=$1 and id < $2 ORDER BY sent DESC LIMIT $3", user.ID.String(), before.String(), count)
		} else if !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender_user=$1 and id > $2 ORDER BY sent DESC LIMIT $3", user.ID.String(), after.String(), count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE sender_user=$1 ORDER BY sent DESC LIMIT $2", user.ID.String(), count)
		}
	case RoleReceiver:
		if !before.IsZero() && !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver_user=$1 and id < $2 and id > $3 ORDER BY sent DESC LIMIT $4", user.ID.String(), before.String(), after.String(), count)
		} else if !before.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver_user=$1 and id < $2 ORDER BY sent DESC LIMIT $3", user.ID.String(), before.String(), count)
		} else if !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver_user=$1 and id > $2 ORDER BY sent DESC LIMIT $3", user.ID.String(), after.String(), count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE receiver_user=$1 ORDER BY sent DESC LIMIT $2", user.ID.String(), count)
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

func (p *Persister) GetLink(id ID) (Link, error) {
	var link Link
	row := p.Database.QueryRow("SELECT * FROM links INNER JOIN urls ON (links.url = urls.id) WHERE id=$1", id.String())
	err := link.fromRow(row)
	if err == sql.ErrNoRows {
		err = LinkNotFoundError
	}
	return link, err
}

func (p *Persister) AddLinks(links []Link) ([]Link, error) {
	urls := map[ID]*URL{}
	url_counts := map[ID]int64{}
	url_links := map[ID][]*Link{}
	for pos, link := range links {
		id, err := p.GetID()
		if err != nil {
			return []Link{}, err
		}
		if link.URL == nil {
			return []Link{}, NilURLError
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
			url_links[link.URL.ID] = []*Link{&link}
		} else {
			url_counts[link.URL.ID] = url_counts[link.URL.ID] + 1
			url_links[link.URL.ID] = append(url_links[link.URL.ID], &link)
		}

		linkID, err := p.GetID()
		if err != nil {
			return []Link{}, err
		}
		links[pos].ID = linkID
		links[pos].Sent = time.Now()
	}
	for _, url := range urls {
		stmt := `INSERT INTO urls VALUES($1, $2, $3, $4);`
		_, err := p.Database.Exec(stmt, url.ID.String(), url.Address, url_counts[url.ID], url.FirstSeen)
		if err != nil {
			if isUniqueConflictError(err) {
				row := p.Database.QueryRow("SELECT * FROM urls WHERE address=$1", url.Address)
				newURL := &URL{}
				err = newURL.fromRow(row)
				if err != nil {
					return []Link{}, err
				}
				newURL.SentCounter = newURL.SentCounter + url_counts[url.ID]
				stmt := `UPDATE urls SET sent_counter=(sent_counter + $1) WHERE id=$2;`
				_, err = p.Database.Exec(stmt, url_counts[url.ID], newURL.ID.String())
				if err != nil {
					return []Link{}, err
				}
				for _, l := range url_links[url.ID] {
					l.URL = newURL
				}
			} else {
				return []Link{}, err
			}
		}
	}
	for _, link := range links {
		stmt := `INSERT INTO links VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);`
		var comment *string
		var read *time.Time
		if link.Comment == "" {
			comment = nil
		} else {
			comment = &link.Comment
		}
		if link.TimeRead.IsZero() {
			read = nil
		} else {
			read = &link.TimeRead
		}
		_, err := p.Database.Exec(stmt, link.ID.String(), link.URL.ID.String(), link.Unread, read, link.Sender.String(), link.SenderUser.String(), link.Receiver.String(), link.ReceiverUser.String(), comment, link.Sent)
		if err != nil {
			return []Link{}, err
		}
	}
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
		_, err := p.Database.Exec(stmt, link.Comment, link.Unread, link.ID.String())
		return link, err
	}
	link.Unread = unread
	stmt := `UPDATE links SET unread=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, link.Unread, link.ID.String())
	return link, err
}

func (p *Persister) DeleteLink(link Link) error {
	stmt := `DELETE FROM links WHERE id=$1;`
	_, err := p.Database.Exec(stmt, link.ID.String())
	if err != nil {
		return err
	}
	stmt = `UPDATE urls SET sent_counter=(sent_counter - 1) WHERE ID=$1;`
	_, err = p.Database.Exec(stmt, link.URL.ID.String())
	return err
}
