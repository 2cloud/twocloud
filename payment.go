package twocloud

import (
	"database/sql"
	"errors"
	"github.com/lib/pq"
	"strconv"
	"strings"
	"time"
)

var PaymentTableCreateStatement = `CREATE TABLE payments (
	id varchar primary key,
	remote_id varchar NOT NULL,
	amount bigint NOT NULL,
	message varchar NOT NULL,
	created timestamp default CURRENT_TIMESTAMP,
	completed timestamp,
	user_id varchar,
	funding_source_id varchar,
	anonymous bool default false,
	campaign varchar,
	status varchar NOT NULL,
	error varchar NOT NULL);`

type Payment struct {
	ID              ID        `json:"id,omitempty"`
	RemoteID        string    `json:"remote_id,omitempty"`
	Amount          int       `json:"amount,omitempty"`
	Message         string    `json:"message,omitempty"`
	Created         time.Time `json:"created,omitempty"`
	Completed       time.Time `json:"completed,omitempty"`
	UserID          ID        `json:"user_id,omitempty"`
	FundingSourceID ID        `json:"funding_source_id,omitempty"`
	Anonymous       bool      `json:"anonymous,omitempty"`
	Campaign        ID        `json:"campaign,omitempty"`
	Status          string    `json:"status,omitempty"`
	Error           string    `json:"error,omitempty"`
}

func (payment *Payment) fromRow(row ScannableRow) error {
	var idStr string
	var userIDStr, fsIDStr, campaignStr *string
	var completed pq.NullTime
	err := row.Scan(&idStr, &payment.RemoteID, &payment.Amount, &payment.Message, &payment.Created, &completed, &userIDStr, &fsIDStr, &payment.Anonymous, &campaignStr, &campaign.Status, &campaign.Error)
	if err != nil {
		return err
	}
	id, err := IDFromString(idStr)
	if err != nil {
		return err
	}
	payment.ID = id
	if userIDStr != nil {
		uid, err := IDFromString(*userIDStr)
		if err != nil {
			return err
		}
		payment.UserID = uid
	}
	if fsIDStr != nil {
		fsID, err := IDFromString(*fsIDStr)
		if err != nil {
			return err
		}
		payment.FundingSourceID = fsID
	}
	if campaignStr != nil {
		campaign, err := IDFromString(*campaignStr)
		if err != nil {
			return err
		}
		payment.Campaign = campaign
	}
	if completed.Valid {
		payment.Completed = completed
	}
	return nil
}

const (
	PAYMENT_STATUS_PENDING   = "pending"
	PAYMENT_STATUS_CHARGING  = "charging"
	PAYMENT_STATUS_REFUNDING = "refunding"
	PAYMENT_STATUS_REFUNDED  = "refunded"
	PAYMENT_STATUS_SUCCESS   = "succeeded"
	PAYMENT_STATUS_ERROR     = "error"
	PAYMENT_STATUS_RETRY     = "retry"
)

var PaymentNotFoundError = errors.New("Payment not found.")
var PaymentInvalidStatusError = errors.New("Invalid status.")

func (p *Persister) GetPayments(before, after ID, count int, status []string, user, campaign *ID) ([]Payment, error) {
	for _, s := range status {
		if s != PAYMENT_STATUS_PENDING && s != PAYMENT_STATUS_CHARGING && s != PAYMENT_STATUS_REFUNDING && s != PAYMENT_STATUS_REFUNDED && s != PAYMENT_STATUS_SUCCESS && s != PAYMENT_STATUS_ERROR && s != PAYMENT_STATUS_RETRY {
			return []Payment{}, PaymentInvalidStatusError
		}
	}
	payments := []Payment{}
	var rows *sql.Rows
	var err error
	keys := []string{}
	values := []interface{}{}
	if !before.IsZero() {
		keys = append(keys, "id <")
		values = append(values, before.String())
	}
	if !after.IsZero() {
		keys = append(keys, "id >")
		values = append(values, after.String())
	}
	if user != nil {
		keys = append(keys, "user_id =")
		values = append(values, user.String())
	}
	if campaign != nil {
		keys = append(keys, "campaign =")
		values = append(values, campaign.String())
	}
	if len(status) > 0 {
		keys = append(keys, "status IN ")
		//TODO: This is HORRIBLE. These should each be separate values that are substituted individually to avoid SQL injection. This method should be used with caution and only on validated things until we get better SQL generation in place.
		values = append(values, "("+strings.Join(status, ", ")+")")
	}
	query := "SELECT * FROM payments"
	if len(keys) {
		query = query + " WHERE "
	}
	for index, key := range keys {
		query = query + key + " $" + strconv.Itoa(index+1) + " "
		if index < len(keys) {
			query = query + "and "
		}
	}
	values = append(values, count)
	query = query + "ORDER BY created DESC LIMIT $" + strconv.Itoa(len(keys))
	rows, err = p.Database.Query(query, values...)
	if err != nil {
		return []Payment{}, err
	}
	for rows.Next() {
		var payment Payment
		err = payment.fromRow(rows)
		if err != nil {
			return []Payment{}, err
		}
		payments = append(payments, payment)
	}
	err = rows.Err()
	return payments, err
}

func (p *Persister) GetPayment(id ID) (Payment, error) {
	var payment Payment
	query := "SELECT * FROM payments WHERE id=$1"
	row := p.Database.QueryRow(query, id.String())
	err := payment.fromRow(row)
	if err == sql.ErrNoRows {
		err = PaymentNotFoundError
	}
	return payment, err
}

func (p *Persister) AddPayment(amount int, message string, userID, fsID, campaignID ID, anonymous bool) (Payment, error) {
	return Payment{}, nil
}

func (p *Persister) UpdatePayment(payment *Payment, amount int, message string, userID, fsID, campaignID ID, anonymous bool) error {
	return nil
}

func (p *Persister) UpdatePaymentStatus(payment *Payment, status, payment_error string, completed bool) error {
	return nil
}

func (p *Persister) DeletePayment(id ID) error {
	return nil
}
