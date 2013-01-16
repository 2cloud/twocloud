package twocloud

import (
	"time"
)

type Subscription struct {
	ID            uint64    `json:"id,omitempty"`
	Active        bool      `json:"active,omitempty"`
	InGracePeriod bool      `json:"in_grace_period,omitempty"`
	Expires       time.Time `json:"expires,omitempty"`
	AutoRenew     bool      `json:"auto_renew,omitempty"`
	FundingID     string    `json:"funding_id,omitempty"`
	FundingSource string    `json:"funding_source,omitempty"`
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

func (r *RequestBundle) updateSubscriptionStatus(user User) {
	user.Subscription.Active = user.Subscription.Expires.After(time.Now())
	grace := user.Subscription.Expires.Add(time.Hour * 24 * r.Config.GracePeriod)
	user.Subscription.InGracePeriod = !user.Subscription.Active && grace.After(time.Now())
}

// TODO: Need to create new subscription and persist it
func (r *RequestBundle) CreateSubscription(user User, funding_id, funding_src string) error {
	return nil
}

// TODO: Need to store the payment source information
func (r *RequestBundle) UpdateSubscriptionPaymentSource(user User, funding_id, funding_src string) error {
	return nil
}

// TODO: Need to store the new expiration date
func (r *RequestBundle) UpdateSubscriptionExpiration(user User, expires time.Time) error {
	user.Subscription.Expires = expires
	return nil
}

// TODO: Need to set autorenew to true and save it
func (r *RequestBundle) StartSubscription(user User) error {
	return nil
}

// TODO: Need to set autorenew to false and save it
func (r *RequestBundle) CancelSubscription(user User) error {
	return nil
}

// TODO: Need to query subscriptions
func (r *RequestBundle) GetSubscriptionsByExpiration(after, before time.Time, count int) ([]Subscription, error) {
	return []Subscription{}, nil
}
