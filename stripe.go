package twocloud

import (
	"database/sql"
	"github.com/lib/pq"
	"time"
)

var StripeTableCreateStatement = `CREATE TABLE stripe (
	id varchar primary key,
	remote_id varchar NOT NULL,
	nickname varchar,
	last_used timestamp,
	added timestamp default CURRENT_TIMESTAMP,
	user_id varchar);`

type Stripe struct {
	FundingSource
}

func (s *Stripe) IsEmpty() bool {
	return s.ID.IsZero()
}

func (s *Stripe) fromRow(row ScannableRow) error {
	var idStr, userIDstr string
	var last_used pq.NullTime
	err := row.Scan(&idStr, &s.RemoteID, &s.Nickname, &last_used, &s.Added, &userIDstr)
	if err != nil {
		return err
	}
	id, err := IDFromString(idStr)
	if err != nil {
		return err
	}
	s.ID = id
	userID, err := IDFromString(userIDstr)
	if err != nil {
		return err
	}
	s.UserID = userID
	if last_used.Valid {
		s.LastUsed = last_used.Time
	}
	return nil
}

func (p *Persister) GetStripeSourcesByUser(user User) ([]Stripe, error) {
	sources := []Stripe{}
	rows, err := p.Database.Query("SELECT * FROM stripe WHERE user_id=$1 ORDER BY last_used DESC", user.ID.String())
	if err != nil {
		return []Stripe{}, err
	}
	for rows.Next() {
		var s Stripe
		err = s.fromRow(rows)
		if err != nil {
			return []Stripe{}, err
		}
		sources = append(sources, s)
	}
	err = rows.Err()
	return sources, err
}

func (p *Persister) GetStripeSource(id ID) (Stripe, error) {
	var s Stripe
	row := p.Database.QueryRow("SELECT * FROM stripe WHERE id=$1", id.String())
	err := s.fromRow(row)
	if err == sql.ErrNoRows {
		err = FundingSourceNotFoundError
	}
	return s, err
}

func (p *Persister) AddStripeSource(remote_id string, nickname string, user_id ID) (Stripe, error) {
	id, err := p.GetID()
	if err != nil {
		return Stripe{}, err
	}
	s := Stripe{
		FundingSource{
			ID:       id,
			RemoteID: remote_id,
			Nickname: &nickname,
			Added:    time.Now(),
			UserID:   user_id,
		},
	}
	stmt := `INSERT INTO stripe VALUES($1, $2, $3, $4, $5, $6);`
	_, err = p.Database.Exec(stmt, s.ID.String(), s.RemoteID, s.Nickname, s.LastUsed, s.Added, s.UserID.String())
	return s, err
}

func (p *Persister) UpdateStripeSource(s *Stripe, remote_id *string, nickname *string) error {
	if remote_id != nil {
		s.RemoteID = *remote_id
	}
	if nickname != nil {
		s.Nickname = nickname
	}
	if remote_id != nil && nickname != nil {
		stmt := `UPDATE stripe SET nickname=$1, remote_id=$2 WHERE id=$3;`
		_, err := p.Database.Exec(stmt, s.GetNickname(), s.RemoteID, s.ID.String())
		return err
	} else if remote_id != nil {
		stmt := `UPDATE stripe SET remote_id=$1 WHERE id=$2;`
		_, err := p.Database.Exec(stmt, s.RemoteID, s.ID.String())
		return err
	} else if nickname != nil {
		stmt := `UPDATE stripe SET nickname=$1 WHERE id=$2;`
		_, err := p.Database.Exec(stmt, s.GetNickname(), s.ID.String())
		return err
	}
	return nil
}

func (p *Persister) UpdateStripeSourceLastUsed(s *Stripe) error {
	s.LastUsed = time.Now()
	stmt := `UPDATE stripe SET last_used=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, s.LastUsed, s.ID.String())
	return err
}

func (p *Persister) DeleteStripeSource(s Stripe) error {
	stmt := `DELETE FROM stripe WHERE id=$1;`
	_, err := p.Database.Exec(stmt, s.ID.String())
	return err
}

func (s *Stripe) Charge(amount int) error {
	// TODO: implement charging a stripe account
	return nil
}
