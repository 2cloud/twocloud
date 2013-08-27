package twocloud

import (
	"errors"
	"github.com/lib/pq"
	"secondbit.org/pan"
	"strings"
	"time"
)

var SubscriptionTableCreateStatement = `CREATE TABLE subscriptions (
	id varchar primary key,
	amount bigint NOT NULL,
	period varchar NOT NULL,
	renews timestamp NOT NULL,
	notify_on_renewal bool NOT NULL,
	last_notified timestamp,
	funding_id varchar NOT NULL,
	funding_source varchar NOT NULL,
	user_id varchar NOT NULL,
	campaign varchar NOT NULL);`

type Subscription struct {
	ID              ID        `json:"id,omitempty"`
	Amount          uint64    `json:"amount,omitempty"`
	Period          string    `json:"period,omitempty"`
	Renews          time.Time `json:"renews,omitempty"`
	NotifyOnRenewal bool      `json:"notify_on_renewal,omitempty"`
	LastNotified    time.Time `json:"last_notified,omitempty"`
	FundingID       ID        `json:"funding_id,omitempty"`
	FundingSource   string    `json:"funding_source,omitempty"`
	UserID          ID        `json:"user_id,omitempty"`
	CampaignID      ID        `json:"campaign,omitempty"`
}

func (subscription *Subscription) IsEmpty() bool {
	return subscription.ID.IsZero()
}

func (subscription *Subscription) CheckValues() error {
	subscription.Period = strings.ToLower(subscription.Period)
	if subscription.Period != "monthly" && subscription.Period != "yearly" {
		return InvalidPeriodError
	}

	subscription.FundingSource = strings.ToLower(subscription.FundingSource)
	if subscription.FundingSource != "stripe" {
		return UnrecognisedFundingSourceError
	}
	return nil
}

var UnrecognisedFundingSourceError = errors.New("Unrecognised funding source.")
var InvalidPeriodError = errors.New("Invalid period.")
var InvalidStatusError = errors.New("Invalid status.")

func (subscription *Subscription) fromRow(row ScannableRow) error {
	var idStr, fundingIDStr, userIDStr, campaignIDStr string
	var lastNotified pq.NullTime
	err := row.Scan(&idStr, &subscription.Amount, &subscription.Period, &subscription.Renews, &subscription.NotifyOnRenewal, &lastNotified, &fundingIDStr, &subscription.FundingSource, &userIDStr, &campaignIDStr)
	if err != nil {
		return err
	}
	subscription.ID, err = IDFromString(idStr)
	if err != nil {
		return err
	}
	subscription.UserID, err = IDFromString(userIDStr)
	if err != nil {
		return err
	}
	subscription.FundingID, err = IDFromString(fundingIDStr)
	if err != nil {
		return err
	}
	subscription.CampaignID, err = IDFromString(campaignIDStr)
	if err != nil {
		return err
	}
	if lastNotified.Valid {
		subscription.LastNotified = lastNotified.Time
	}
	return nil
}

func (p *Persister) CreateSubscription(amount uint64, period string, renews time.Time, notify bool, campaign_id, user_id, funding_id ID, funding_src string) (*Subscription, error) {
	id, err := p.GetID()
	if err != nil {
		return nil, err
	}
	period = strings.ToLower(period)
	if renews.IsZero() {
		renews = time.Now()
		if period == "monthly" {
			renews = renews.Add(time.Hour * 24 * 30)
		} else if period == "yearly" {
			renews = renews.Add(time.Hour * 24 * 365)
		} else {
			return nil, InvalidPeriodError
		}
	} else {
		if period != "monthly" && period != "yearly" {
			return nil, InvalidPeriodError
		}
	}
	stmt := `INSERT INTO subscriptions VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);`
	_, err = p.Database.Exec(stmt, id.String(), amount, period, renews, notify, nil, funding_id.String, funding_src, user_id.String(), campaign_id.String())
	subscription := &Subscription{
		ID:              id,
		Amount:          amount,
		Period:          period,
		Renews:          renews,
		NotifyOnRenewal: notify,
		FundingID:       funding_id,
		FundingSource:   funding_src,
		UserID:          user_id,
		CampaignID:      campaign_id,
	}
	return subscription, err
}

func (p *Persister) UpdateSubscription(sub *Subscription, amount *uint64, period *string, renews *time.Time, notify *bool, campaign, user, fundingID *ID, fundingSource *string) error {
	query := pan.New()
	query.SQL = "UPDATE subscriptions SET "
	vars := []string{}
	if amount != nil {
		vars = append(vars, "amount=?")
		query.Args = append(query.Args, *amount)
		sub.Amount = *amount
	}
	if period != nil {
		periodStr := strings.ToLower(*period)
		if periodStr != "monthly" && periodStr != "yearly" {
			return InvalidPeriodError
		}
		vars = append(vars, "period=?")
		query.Args = append(query.Args, periodStr)
		sub.Period = *period
	}
	if renews != nil {
		vars = append(vars, "renews=?")
		query.Args = append(query.Args, *renews)
		sub.Renews = *renews
	}
	if notify != nil {
		vars = append(vars, "notify_on_renewal=?")
		query.Args = append(query.Args, *notify)
		sub.NotifyOnRenewal = *notify
	}
	if campaign != nil {
		vars = append(vars, "campaign_id=?")
		query.Args = append(query.Args, campaign.String())
		sub.CampaignID = *campaign
	}
	if user != nil {
		vars = append(vars, "user_id=?")
		query.Args = append(query.Args, user.String())
		sub.UserID = *user
	}
	if fundingID != nil {
		vars = append(vars, "funding_id=?")
		query.Args = append(query.Args, fundingID.String())
		sub.FundingID = *fundingID
	}
	if fundingSource != nil {
		vars = append(vars, "funding_source=?")
		query.Args = append(query.Args, *fundingSource)
		sub.FundingSource = *fundingSource
	}
	query.SQL += strings.Join(vars, " and ") + " "
	query.IncludeWhere()
	query.SQL += "id=?"
	query.Args = append(query.Args, sub.ID.String())
	_, err := p.Database.Exec(query.String(), query.Args...)
	return err
}

func (p *Persister) GetSubscriptionsByExpiration(status string, after, before ID, count int) ([]Subscription, error) {
	subscriptions := []Subscription{}
	query := pan.New()
	query.SQL = "SELECT * FROM subscriptions"
	if status != "" {
		query.IncludeWhere()
		status = strings.ToLower(status)
		if status == "renewing" {
			query.SQL += "renews < ?"
			query.Args = append(query.Args, time.Now())
		} else if status == "renewing_soon" {
			query.SQL += "renews > ? AND renews < ?"
			query.Args = append(query.Args, time.Now(), time.Now().Add(24*time.Hour))
		} else {
			return subscriptions, InvalidStatusError
		}
	}
	if !after.IsZero() {
		query.IncludeWhere()
		query.SQL += "id > ?"
		query.Args = append(query.Args, after.String())
	}
	if !before.IsZero() {
		query.IncludeWhere()
		query.SQL += "id < ?"
		query.Args = append(query.Args, before.String())
	}
	query.IncludeOrder()
	query.SQL += "renews DESC"
	query.IncludeLimit(count)
	rows, err := p.Database.Query(query.String(), query.Args...)
	if err != nil {
		return subscriptions, err
	}
	for rows.Next() {
		subscription := Subscription{}
		err = subscription.fromRow(rows)
		if err != nil {
			return subscriptions, err
		}
		subscriptions = append(subscriptions, subscription)
	}
	err = rows.Err()
	return subscriptions, err
}

func (p *Persister) GetSubscriptionsByUser(user ID, after, before ID, count int) ([]Subscription, error) {
	subscriptions := []Subscription{}
	query := pan.New()
	query.SQL = "SELECT * FROM subscriptions"
	query.IncludeWhere()
	query.SQL += "user_id=?"
	query.Args = append(query.Args, user.String())
	if !after.IsZero() {
		query.IncludeWhere()
		query.SQL += "id > ?"
		query.Args = append(query.Args, after.String())
	}
	if !before.IsZero() {
		query.IncludeWhere()
		query.SQL += "id < ?"
		query.Args = append(query.Args, before.String())
	}
	query.IncludeOrder()
	query.SQL += "renews DESC"
	query.IncludeLimit(count)
	rows, err := p.Database.Query(query.String(), query.Args...)
	if err != nil {
		return subscriptions, err
	}
	for rows.Next() {
		var subscription Subscription
		err := subscription.fromRow(rows)
		if err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, subscription)
	}
	err = rows.Err()
	return subscriptions, err
}

func (p *Persister) GetSubscription(id ID) (*Subscription, error) {
	subscription := &Subscription{}
	query := pan.New()
	query.SQL = "SELECT * FROM subscriptions"
	query.IncludeWhere()
	query.SQL += "id=?"
	query.Args = append(query.Args, id.String())
	row := p.Database.QueryRow(query.String(), query.Args...)
	err := subscription.fromRow(row)
	if err != nil {
		return nil, err
	}
	return subscription, nil
}

func (p *Persister) CancelSubscription(id ID) error {
	query := pan.New()
	query.SQL = "DELETE FROM subscriptions"
	query.IncludeWhere()
	query.SQL += "id=?"
	query.Args = append(query.Args, id.String())
	_, err := p.Database.Exec(query.String(), query.Args...)
	return err
}

func (p *Persister) cancelSubscriptionsByUser(user ID) error {
	query := pan.New()
	query.SQL = "DELETE FROM subscriptions"
	query.IncludeWhere()
	query.SQL += "user_id=?"
	query.Args = append(query.Args, user.String())
	_, err := p.Database.Exec(query.String(), query.Args...)
	return err
}
