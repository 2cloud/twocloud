package twocloud

import (
	"database/sql"
	"errors"
	"github.com/PuerkitoBio/purell"
	"github.com/lib/pq"
	"secondbit.org/pan"
	"strings"
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
	Comment      *string   `json:"comment,omitempty"`
	Sent         time.Time `json:"sent,omitempty"`
}

func (link *Link) fromRow(row ScannableRow) error {
	var read pq.NullTime
	var idStr, urlIDStr, senderIDStr, senderUserIDStr, receiverIDStr, receiverUserIDStr string
	link.URL = &URL{}
	err := row.Scan(&idStr, &urlIDStr, &link.Unread, &read, &senderIDStr, &senderUserIDStr, &receiverIDStr, &receiverUserIDStr, &link.Comment, &link.Sent, &urlIDStr, &link.URL.Address, &link.URL.SentCounter, &link.URL.FirstSeen)
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

const (
	LinkCreatedTopic = "links.created"
	LinkUpdatedTopic = "links.updated"
	LinkDeletedTopic = "links.deleted"
)

var NilURLError = errors.New("Links must supply a URL.")
var LinkNotFoundError = errors.New("Link not found.")

func (p *Persister) GetLinksByDevice(device Device, role RoleFlag, before, after ID, count int) ([]Link, error) {
	links := []Link{}
	query := pan.New()
	query.SQL = "SELECT * FROM links INNER JOIN urls ON (links.url = urls.id)"
	query.IncludeWhere()
	switch role {
	case RoleEither:
		query.Include("(sender=? or receiver=?)", device.ID.String(), device.ID.String())
	case RoleSender:
		query.Include("sender=?", device.ID.String())
	case RoleReceiver:
		query.Include("receiver=?", device.ID.String())
	}
	query.IncludeIfNotEmpty("id < ?", before)
	query.IncludeIfNotEmpty("id > ?", after)
	query.FlushExpressions(" and ")
	query.IncludeOrder("sent DESC")
	query.IncludeLimit(count)
	rows, err := p.Database.Query(query.Generate(" "), query.Args...)
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
	query := pan.New()
	query.SQL = "SELECT * FROM links INNER JOIN urls on (links.url = urls.id)"
	query.IncludeWhere()
	switch role {
	case RoleEither:
		query.Include("(sender=? or receiver=?)", user.ID.String())
	case RoleSender:
		query.Include("sender=?", user.ID.String())
	case RoleReceiver:
		query.Include("receiver=?", user.ID.String())
	}
	query.IncludeIfNotEmpty("id < ?", before)
	query.IncludeIfNotEmpty("id > ?", after)
	query.FlushExpressions(" and ")
	query.IncludeOrder("sent DESC")
	query.IncludeLimit(count)
	rows, err := p.Database.Query(query.Generate(" "), query.Args...)
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
	query := pan.New()
	query.SQL = "SELECT * FROM links INNER JOIN urls ON (links.url = urls.id)"
	query.IncludeWhere()
	query.Include("id=?", id.String())
	row := p.Database.QueryRow(query.Generate(" "), query.Args...)
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
	query := pan.New()
	for _, url := range urls {
		query.SQL = "INSERT INTO urls VALUES("
		query.Include("?", url.ID.String())
		query.Include("?", url.Address)
		query.Include("?", url_counts[url.ID])
		query.Include("?", url.FirstSeen)
		query.FlushExpressions(", ")
		query.SQL += ")"
		_, err := p.Database.Exec(query.Generate(" "), query.Args...)
		if err != nil {
			if isUniqueConflictError(err) {
				query.SQL = "SELECT * FROM urls"
				query.IncludeWhere()
				query.Include("address=?", url.Address)
				row := p.Database.QueryRow(query.Generate(" "), query.Args...)
				newURL := &URL{}
				err = newURL.fromRow(row)
				if err != nil {
					return []Link{}, err
				}
				newURL.SentCounter = newURL.SentCounter + url_counts[url.ID]
				query.IncludesWhere = false
				query.SQL = "UPDATE urls SET"
				query.Include("sent_counter=(sent_counter + ?)", url_counts[url.ID])
				query.IncludeWhere()
				query.Include("id=?", newURL.ID.String())
				_, err = p.Database.Exec(query.Generate(" "), query.Args...)
				if err != nil {
					return []Link{}, err
				}
				for _, l := range url_links[url.ID] {
					l.URL = newURL
				}
				query.IncludesWhere = false // shouldn't be necessary, but just to be safe
			} else {
				return []Link{}, err
			}
		}
	}
	for _, link := range links {
		var read *time.Time
		if link.TimeRead.IsZero() {
			read = nil
		} else {
			read = &link.TimeRead
		}
		query.SQL = "INSERT INTO links VALUE("
		query.Include("?", link.ID.String())
		query.Include("?", link.URL.ID.String())
		query.Include("?", link.Unread)
		query.Include("?", read)
		query.Include("?", link.Sender.String())
		query.Include("?", link.SenderUser.String())
		query.Include("?", link.Receiver.String())
		query.Include("?", link.ReceiverUser.String())
		query.Include("?", link.Comment)
		query.Include("?", link.Sent)
		query.FlushExpressions(", ")
		query.SQL += ")"
		_, err := p.Database.Exec(query.Generate(" "), query.Args...)
		query.IncludesWhere = false // shouldn't be necessary, but just to be safe
		if err != nil {
			return []Link{}, err
		}
		_, nsqErr := p.Publish(LinkCreatedTopic, &link.ReceiverUser, &link.Receiver, &link.ID)
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return links, nil
}

func (p *Persister) AddLink(address string, comment *string, sender, receiver Device, unread bool) (Link, error) {
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

func (p *Persister) UpdateLink(link Link, unread *bool, comment *string) (Link, error) {
	query := pan.New()
	query.SQL = "UPDATE links SET"
	if comment != nil {
		link.Comment = comment
		query.Include("comment=?", link.Comment)
	}
	if unread != nil {
		link.Unread = *unread
		query.Include("unread=?", link.Unread)
	}
	query.FlushExpressions(", ")
	query.IncludeWhere()
	query.Include("id=?", link.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err == nil {
		_, nsqErr := p.Publish(LinkUpdatedTopic, &link.ReceiverUser, &link.Receiver, &link.ID)
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
		_, nsqErr = p.Publish(LinkUpdatedTopic, &link.SenderUser, &link.Sender, &link.ID)
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return link, err
}

func (p *Persister) DeleteLink(link Link) error {
	return p.DeleteLinks([]Link{link})
}

func (p *Persister) DeleteLinks(links []Link) error {
	query := pan.New()
	query.SQL = "DELETE FROM links"
	query.IncludeWhere()
	queryKeys := make([]string, len(links))
	queryVals := make([]interface{}, len(links))
	for _, link := range links {
		queryKeys = append(queryKeys, "?")
		queryVals = append(queryVals, link.ID.String())
	}
	query.Include("id IN("+strings.Join(queryKeys, ", ")+")", queryVals...)
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err != nil {
		return err
	}
	query.IncludesWhere = false
	query.SQL = "UPDATE urls SET"
	query.Include("sent_count=(sent_count - 1)")
	query.IncludeWhere()
	query.Include("id IN("+strings.Join(queryKeys, ", ")+")", queryVals...)
	_, err = p.Database.Exec(query.Generate(" "), query.Args...)
	if err == nil {
		for _, link := range links {
			_, nsqErr := p.Publish(LinkDeletedTopic, &link.ReceiverUser, &link.Receiver, &link.ID)
			if nsqErr != nil {
				p.Log.Error(nsqErr.Error())
			}
			_, nsqErr = p.Publish(LinkDeletedTopic, &link.SenderUser, &link.Sender, &link.ID)
			if nsqErr != nil {
				p.Log.Error(nsqErr.Error())
			}
		}
	}
	return err
}

func (p *Persister) DeleteLinksByDevices(devices []Device) error {
	query := pan.New()
	query.SQL = "DELETE FROM links"
	query.IncludeWhere()
	queryKeys := make([]string, len(devices))
	queryVals := make([]interface{}, len(devices))
	for _, device := range devices {
		queryKeys = append(queryKeys, "?")
		queryVals = append(queryVals, device.ID.String())
	}
	query.Include("receiver IN("+strings.Join(queryKeys, ", ")+")", queryVals...)
	// BUG(paddyforan): Deleting a device will cause URL counts to be out of sync
	// BUG(paddyforan): Deleting a device will not remove unique URLs from the URL counts
	// BUG(paddyforan): Deleting links by device will not send push notifications
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}

func (p *Persister) DeleteLinksByDevice(device Device) error {
	return p.DeleteLinksByDevices([]Device{device})
}

func (p *Persister) DeleteLinksByUsers(users []User) error {
	query := pan.New()
	query.SQL = "DELETE Drom links"
	query.IncludeWhere()
	queryKeys := make([]string, len(users))
	queryVals := make([]interface{}, len(users))
	for _, user := range users {
		queryKeys = append(queryKeys, "?")
		queryVals = append(queryVals, user.ID.String())
	}
	query.Include("receiver_user IN("+strings.Join(queryKeys, ", ")+")", queryVals...)
	// BUG(paddyforan): Deleting a user will cause URL counts to be out of sync
	// BUG(paddyforan): Deleting a user will not remove unique URLs from the URL counts
	// BUG(paddyforan): Deleting links by user will not send push notifications
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}

func (p *Persister) DeleteLinksByUser(user User) error {
	return p.DeleteLinksByUsers([]User{user})
}
