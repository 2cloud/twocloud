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
	FundingID     uint64    `json:"funding_id,omitempty"`
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

func (r *RequestBundle) UpdateSubscriptionStatus(user User) {
	user.Subscription.Active = user.Subscription.Expires.After(time.Now())
	grace := user.Subscription.Expires.Add(time.Hour * 24 * r.Config.GracePeriod)
	user.Subscription.InGracePeriod = !user.Subscription.Active && grace.After(time.Now())
}

func (r *RequestBundle) CreateSubscription(user User, auth []string) error {
	customerID, err := r.subscriptionIDFromAuthTokens(auth)
	if err != nil {
		r.Log.Error(err.Error())
		return err
	}
	user.Subscription.ID = customerID
	err = r.storeSubscription(user.ID, user.Subscription)
	if err != nil {
		r.Log.Error(err.Error())
		return err
	}
	return nil
}

func (r *RequestBundle) subscriptionIDFromAuthTokens(auth []string) (string, error) {
	if len(auth) < 1 {
		// TODO: throw an error
		return "", nil
	}
	tokenparts := strings.SplitN(auth[0], ":", 2)
	switch tokenparts[0] {
	case "stripe":
		// TODO: create stripe customer
		return "stripe customer id", nil
	}
	// TODO: throw an invalid subscription provider error
	return "", nil
}

func (r *RequestBundle) UpdateSubscriptionPaymentSource(user User, auth []string) error {
	// start instrumentation
	if len(auth) < 1 {
		// TODO: throw an error
		return nil
	}
	idparts := strings.SplitN(user.Subscription.ID, ":", 2)
	tokenparts := strings.SplitN(auth[0], ":", 2)
	if idparts[0] != tokenparts[0] {
		// TODO: cancel subscription
		customerID, err := r.subscriptionIDFromAuthTokens(auth)
		if err != nil {
			r.Log.Error(err.Error())
			return err
		}
		user.Subscription.ID = customerID
		err = r.storeSubscription(user.ID, user.Subscription)
		if err != nil {
			r.Log.Error(err.Error())
			return err
		}
		return nil
	}
	switch tokenparts[0] {
	case "stripe":
		// TODO: update customer payment source
		return nil
	}
	// TODO: throw an invalid subscription provider error
	return nil
}

func (r *RequestBundle) UpdateSubscription(user User, expires time.Time) error {
	// start instrumentation
	user.Subscription.Expires = expires
	err := r.storeSubscription(user.ID, user.Subscription)
	// add repo request to instrumentation
	if err != nil {
		return err
	}
	r.UpdateSubscriptionStatus(user)
	// send the push notification
	// stop instrumentation
	return nil
}

func (r *RequestBundle) CancelSubscription(user User) error {
	return nil
}

func (r *RequestBundle) storeSubscription(userID uint64, subscription *Subscription) error {
	// start instrumentation
	changes := map[string]interface{}{}
	from := map[string]interface{}{}
	old_user, err := r.GetUser(userID)
	// add repo call to instrumentation
	if err != nil {
		return err
	}
	old_sub := old_user.Subscription
	if old_sub.Expires != subscription.Expires {
		changes["subscription_expires"] = subscription.Expires.Format(time.RFC3339)
		from["subscription_expires"] = old_sub.Expires.Format(time.RFC3339)
	}
	if old_sub.ID != subscription.ID {
		changes["subscription_id"] = subscription.ID
		from["subscription_id"] = old_sub.ID
	}
	reply := r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		mc.Hmset("users:"+strconv.FormatUint(userID, 10), changes)
		mc.Zadd("users_by_subscription_expiration", subscription.Expires.Unix(), userID)
	})
	// add repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return reply.Err
	}
	r.AuditMap("users:"+strconv.FormatUint(userID, 10), from, changes)
	// stop instrumentation
	return nil
}

func (r *RequestBundle) GetGraceSubscriptions(after, before uint64, count int) ([]Subscription, error) {
	return []Subscription{}, nil
}
