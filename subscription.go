package twocloud

import (
	"time"
)

var SubscriptionTableCreateStatement = `CREATE TABLE subscriptions (
	id bigint primary key,
	expires timestamp NOT NULL,
	auto_renew bool default false,
	funding_id varchar NOT NULL,
	funding_source varchar NOT NULL);`

type Subscription struct {
	ID            uint64    `json:"id,omitempty"`
	Active        bool      `json:"active,omitempty"`
	InGracePeriod bool      `json:"in_grace_period,omitempty"`
	Expires       time.Time `json:"expires,omitempty"`
	AutoRenew     bool      `json:"auto_renew,omitempty"`
	FundingID     string    `json:"funding_id,omitempty"`
	FundingSource string    `json:"funding_source,omitempty"`
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

func (subscription *Subscription) fromRow(row ScannableRow) error {
	return row.Scan(&subscription.ID, &subscription.Expires, &subscription.AutoRenew, &subscription.FundingID, &subscription.FundingSource)
}

func (p *Persister) updateSubscriptionStatus(subscription *Subscription) {
	subscription.Active = subscription.Expires.After(time.Now())
	grace := subscription.Expires.Add(time.Hour * 24 * p.Config.GracePeriod)
	subscription.InGracePeriod = !subscription.Active && grace.After(time.Now())
}

// TODO: Need to create new subscription and persist it
func (p *Persister) CreateSubscription(user User, funding_id, funding_src string) error {
	return nil
}

// TODO: Need to store the payment source information
func (p *Persister) UpdateSubscriptionPaymentSource(user User, funding_id, funding_src string) error {
	return nil
}

// TODO: Need to store the new expiration date
func (p *Persister) UpdateSubscriptionExpiration(user User, expires time.Time) error {
	user.Subscription.Expires = expires
	return nil
}

// TODO: Need to set autorenew to true and save it
func (p *Persister) StartSubscription(user User) error {
	return nil
}

// TODO: Need to set autorenew to false and save it
func (p *Persister) CancelSubscription(user User) error {
	return nil
}

// TODO: Need to query subscriptions
func (p *Persister) GetSubscriptionsByExpiration(after, before time.Time, count int) ([]Subscription, error) {
	return []Subscription{}, nil
}
