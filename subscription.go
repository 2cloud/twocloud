package twocloud

import (
	"database/sql"
	"errors"
	"time"
)

var SubscriptionTableCreateStatement = `CREATE TABLE subscriptions (
	id varchar primary key,
	expires timestamp NOT NULL,
	auto_renew bool default false,
	funding_id varchar,
	funding_source varchar,
	user_id varchar NOT NULL);`

type Subscription struct {
	ID            ID        `json:"id,omitempty"`
	Active        bool      `json:"active,omitempty"`
	InGracePeriod bool      `json:"in_grace_period,omitempty"`
	Expires       time.Time `json:"expires,omitempty"`
	AutoRenew     bool      `json:"auto_renew,omitempty"`
	FundingID     ID        `json:"funding_id,omitempty"`
	FundingSource string    `json:"funding_source,omitempty"`
	UserID        ID        `json:"user_id,omitempty"`
}

func (subscription *Subscription) IsEmpty() bool {
	return subscription.ID.IsZero()
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
	var fundingIDStr, userIDStr sql.NullString
	var idStr string
	err := row.Scan(&idStr, &subscription.Expires, &subscription.AutoRenew, &fundingIDStr, &subscription.FundingSource, &userIDStr)
	if err != nil {
		return err
	}
	id, err := IDFromString(idStr)
	if err != nil {
		return err
	}
	subscription.ID = id
	subscription.UserID = ID(0)
	subscription.FundingID = ID(0)
	if userIDStr.Valid {
		userID, err := IDFromString(userIDStr.String)
		if err != nil {
			return err
		}
		subscription.UserID = userID
	}
	if fundingIDStr.Valid {
		fundingID, err := IDFromString(fundingIDStr.String)
		if err != nil {
			return err
		}
		subscription.FundingID = fundingID
	}
	return nil
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

func (p *Persister) CreateSubscription(user_id, funding_id ID, funding_src string, auto_renew bool) (*Subscription, error) {
	id, err := p.GetID()
	if err != nil {
		return nil, err
	}
	expires := p.getTrialEnd()
	stmt := `INSERT INTO subscriptions VALUES($1, $2, $3, $4, $5, $6);`
	_, err = p.Database.Exec(stmt, id.String(), expires, auto_renew, funding_id.String(), funding_src, user_id.String())
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

func (p *Persister) UpdateSubscriptionPaymentSource(subscription *Subscription, funding_id ID, funding_src string) error {
	subscription.FundingID = funding_id
	subscription.FundingSource = funding_src
	stmt := `UPDATE subscriptions SET funding_id=$1, funding_source=$2 WHERE id=$3;`
	_, err := p.Database.Exec(stmt, funding_id.String(), funding_src, subscription.ID.String())
	return err
}

func (p *Persister) UpdateSubscriptionExpiration(subscription *Subscription, expires time.Time) error {
	subscription.Expires = expires
	stmt := `UPDATE subscriptions SET expires=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, expires, subscription.ID.String())
	return err
}

func (p *Persister) StartRenewingSubscription(subscription *Subscription) error {
	subscription.AutoRenew = true
	stmt := `UPDATE subscriptions SET auto_renew=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, true, subscription.ID.String())
	return err
}

func (p *Persister) CancelRenewingSubscription(subscription *Subscription) error {
	subscription.AutoRenew = false
	stmt := `UPDATE subscriptions SET auto_renew=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, false, subscription.ID.String())
	return err
}

func (p *Persister) GetSubscriptionsByExpiration(after, before time.Time, count int) ([]*Subscription, error) {
	subscriptions := []*Subscription{}
	var rows *sql.Rows
	var err error
	if !after.IsZero() && !before.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM subscriptions WHERE expires > $1 and expires < $2 ORDER BY expires DESC LIMIT $3", after, before, count)
	} else if !after.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM subscriptions WHERE expires > $1 ORDER BY expires DESC LIMIT $2", after, count)
	} else if !before.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM subscriptions WHERE expires < $1 ORDER BY expires DESC LIMIT $2", before, count)
	} else {
		rows, err = p.Database.Query("SELECT * FROM subscriptions ORDER BY expires DESC LIMIT $1", count)
	}
	if err != nil {
		return []*Subscription{}, err
	}
	for rows.Next() {
		subscription := &Subscription{}
		err = subscription.fromRow(rows)
		if err != nil {
			return []*Subscription{}, err
		}
		p.updateSubscriptionStatus(subscription)
		subscriptions = append(subscriptions, subscription)
	}
	err = rows.Err()
	return subscriptions, err
}

func (p *Persister) GetSubscriptionByUser(user ID) (*Subscription, error) {
	subscription := &Subscription{}
	row := p.Database.QueryRow("SELECT * FROM subscriptions WHERE user_id=$1", user.String())
	err := subscription.fromRow(row)
	if err != nil {
		return nil, err
	}
	p.updateSubscriptionStatus(subscription)
	return subscription, nil
}

func (p *Persister) GetSubscription(id ID) (*Subscription, error) {
	subscription := &Subscription{}
	row := p.Database.QueryRow("SELECT * FROM subscriptions WHERE id=$1", id.String())
	err := subscription.fromRow(row)
	if err != nil {
		return nil, err
	}
	p.updateSubscriptionStatus(subscription)
	return subscription, nil
}

func (p *Persister) deleteSubscription(id ID) error {
	stmt := `DELETE FROM subscriptions WHERE id=$1;`
	_, err := p.Database.Exec(stmt, id.String())
	return err
}

func (p *Persister) deleteSubscriptionByUser(user ID) error {
	stmt := `DELETE FROM subscriptions WHERE user_id=$1;`
	_, err := p.Database.Exec(stmt, user.String())
	return err
}
