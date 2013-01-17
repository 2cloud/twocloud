package twocloud

import (
	"errors"
	"time"
)

var SubscriptionTableCreateStatement = `CREATE TABLE subscriptions (
	id bigint primary key,
	expires timestamp NOT NULL,
	auto_renew bool default false,
	funding_id bigint,
	funding_source varchar,
	user_id bigint NOT NULL);`

type Subscription struct {
	ID            uint64    `json:"id,omitempty"`
	Active        bool      `json:"active,omitempty"`
	InGracePeriod bool      `json:"in_grace_period,omitempty"`
	Expires       time.Time `json:"expires,omitempty"`
	AutoRenew     bool      `json:"auto_renew,omitempty"`
	FundingID     uint64    `json:"funding_id,omitempty"`
	FundingSource string    `json:"funding_source,omitempty"`
	UserID        uint64    `json:"user_id,omitempty"`
}

func (subscription *Subscription) IsEmpty() bool {
	return subscription.ID == 0
}

type SubscriptionExpiredError struct {
	Expired time.Time
}

func (e *SubscriptionExpiredError) Error() string {
	specifics := ""
	if !e.Expired.IsZero() {
		specifics = " It expired on " + e.Expired.Format("Jan 02, 2006") + "."
	}
	return "Your subscription has expired." + specifics
}

type SubscriptionExpiredWarning struct {
	Expired time.Time
}

func (e *SubscriptionExpiredWarning) Error() string {
	specifics := ""
	if !e.Expired.IsZero() {
		specifics = " It expired on " + e.Expired.Format("Jan 02, 2006") + "."
	}
	return "Warning! Your subscription has expired." + specifics
}

var UnrecognisedFundingSourceError = errors.New("Unrecognised funding source.")

func (subscription *Subscription) fromRow(row ScannableRow) error {
	return row.Scan(&subscription.ID, &subscription.Expires, &subscription.AutoRenew, &subscription.FundingID, &subscription.FundingSource, &subscription.UserID)
}

func (p *Persister) Charge(subscription *Subscription, amount int) error {
	switch subscription.FundingSource {
	case "dwolla":
		// TODO: retrieve dwolla funding information
		// TODO: put message on the dwolla subscription queue
	case "stripe":
		// TODO: retrieve stripe funding information
		// TODO: put message on the stripe subscription queue
	default:
		return UnrecognisedFundingSourceError
	}
	return nil
}

func (p *Persister) updateSubscriptionStatus(subscription *Subscription) {
	subscription.Active = subscription.Expires.After(time.Now())
	grace := subscription.Expires.Add(time.Hour * 24 * p.Config.GracePeriod)
	subscription.InGracePeriod = !subscription.Active && grace.After(time.Now())
}

func (p *Persister) getTrialEnd() time.Time {
	return time.Now().Add(time.Hour * 24 * p.Config.GracePeriod)
}

func (p *Persister) CreateSubscription(user_id, funding_id uint64, funding_src string, auto_renew bool) (*Subscription, error) {
	id, err := p.GetID()
	if err != nil {
		return nil, err
	}
	expires := p.getTrialEnd()
	stmt := `INSERT INTO subscriptions VALUES($1, $2, $3, $4, $5, $6);`
	_, err = p.Database.Exec(stmt, id, expires, auto_renew, funding_id, funding_src, user_id)
	subscription := &Subscription{
		ID:            id,
		Expires:       expires,
		AutoRenew:     auto_renew,
		FundingID:     funding_id,
		FundingSource: funding_src,
		UserID:        user_id,
	}
	p.updateSubscriptionStatus(subscription)
	return subscription, err
}

func (p *Persister) UpdateSubscriptionPaymentSource(subscription *Subscription, funding_id uint64, funding_src string) error {
	subscription.FundingID = funding_id
	subscription.FundingSource = funding_src
	stmt := `UPDATE subscriptions SET funding_id=$1, funding_source=$2 WHERE id=$3;`
	_, err := p.Database.Exec(stmt, funding_id, funding_src, subscription.ID)
	return err
}

func (p *Persister) UpdateSubscriptionExpiration(subscription *Subscription, expires time.Time) error {
	subscription.Expires = expires
	stmt := `UPDATE subscriptions SET expires=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, expires, subscription.ID)
	return err
}

func (p *Persister) StartRenewingSubscription(subscription *Subscription) error {
	subscription.AutoRenew = true
	stmt := `UPDATE subscriptions SET auto_renew=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, true, subscription.ID)
	return err
}

func (p *Persister) CancelRenewingSubscription(subscription *Subscription) error {
	subscription.AutoRenew = false
	stmt := `UPDATE subscriptions SET auto_renew=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, false, subscription.ID)
	return err
}

func (p *Persister) GetSubscriptionsByExpiration(after, before time.Time, count int) ([]*Subscription, error) {
	subscriptions := []*Subscription{}
	rows, err := p.Database.Query("SELECT * FROM subscriptions WHERE expires > $1 and expires < $2 LIMIT $3", after, before, count)
	if err != nil {
		return []*Subscription{}, err
	}
	for rows.Next() {
		subscription := &Subscription{}
		err = subscription.fromRow(rows)
		if err != nil {
			return []*Subscription{}, err
		}
		subscriptions = append(subscriptions, subscription)
	}
	err = rows.Err()
	return subscriptions, err
}

func (p *Persister) GetSubscriptionByUser(user uint64) (*Subscription, error) {
	subscription := &Subscription{}
	row := p.Database.QueryRow("SELECT * FROM subscriptions WHERE user_id=$1", user)
	err := subscription.fromRow(row)
	return subscription, err
}

func (p *Persister) GetSubscription(id uint64) (*Subscription, error) {
	subscription := &Subscription{}
	row := p.Database.QueryRow("SELECT * FROM subscriptions WHERE id=$1", id)
	err := subscription.fromRow(row)
	return subscription, err
}

func (p *Persister) deleteSubscription(id uint64) error {
	stmt := `DELETE FROM subscriptions WHERE id=$1;`
	_, err := p.Database.Exec(stmt, id)
	return err
}

func (p *Persister) deleteSubscriptionByUser(user uint64) error {
	stmt := `DELETE FROM subscriptions WHERE user_id=$1;`
	_, err := p.Database.Exec(stmt, user)
	return err
}
