package twocloud

import (
	"database/sql"
	"github.com/bradrydzewski/go.stripe"
	"github.com/lib/pq"
	"secondbit.org/pan"
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
	query := pan.New()
	query.SQL += "SELECT * FROM stripe"
	query.IncludeWhere()
	query.Include("user_id=?", user.ID.String())
	query.IncludeOrder("last_used DESC")
	rows, err := p.Database.Query(query.Generate(" "), query.Args...)
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
	query := pan.New()
	query.SQL = "SELECT * FROM stripe"
	query.IncludeWhere()
	query.Include("id=?", id.String())
	row := p.Database.QueryRow(query.Generate(" "), query.Args...)
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
	query := pan.New()
	query.SQL = "INSERT INTO stripe VALUES("
	query.Include("?", s.ID.String())
	query.Include("?", s.RemoteID)
	query.Include("?", s.Nickname)
	query.Include("?", s.LastUsed)
	query.Include("?", s.Added)
	query.Include("?", s.UserID.String())
	query.FlushExpressions(", ")
	query.SQL += ")"
	_, err = p.Database.Exec(query.Generate(" "), query.Args...)
	return s, err
}

func (p *Persister) UpdateStripeSource(s *Stripe, remote_id *string, nickname *string) error {
	query := pan.New()
	query.SQL = "UPDATE stripe SET "
	if remote_id != nil {
		s.RemoteID = *remote_id
		query.Include("remote_id=?", s.RemoteID)
	}
	if nickname != nil {
		s.Nickname = nickname
		query.Include("nickname=?", s.Nickname)
	}
	query.FlushExpressions(", ")
	query.IncludeWhere()
	query.Include("id=?", s.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}

func (p *Persister) UpdateStripeSourceLastUsed(s *Stripe) error {
	s.LastUsed = time.Now()
	query := pan.New()
	query.SQL = "UPDATE stripe SET"
	query.Include("last_used=?", s.LastUsed)
	query.IncludeWhere()
	query.Include("id=?", s.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}

func (p *Persister) DeleteStripeSource(s Stripe) error {
	query := pan.New()
	query.SQL = "DELETE FROM stripe"
	query.IncludeWhere()
	query.Include("id=?", s.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}

func (p *Persister) ChargeStripe(s *Stripe, amount int64) error {
	if s == nil {
		return FundingSourceNilError
	}
	if s.RemoteID == "" {
		return FundingSourceEmptyRemoteIDError
	}
	params := stripe.ChargeParams{
		Amount:   amount,
		Currency: stripe.USD,
		Customer: s.RemoteID,
	}
	charge, err := stripe.Charges.Create(&params)
	if err != nil {
		return err
	}
	p.Log.Debug("Created charge %s", charge.Id)
	return p.UpdateStripeSourceLastUsed(s)
}
