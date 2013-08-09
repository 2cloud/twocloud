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
	err := row.Scan(&idStr, &payment.RemoteID, &payment.Amount, &payment.Message, &payment.Created, &completed, &userIDStr, &fsIDStr, &payment.Anonymous, &campaignStr, &payment.Status, &payment.Error)
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
		payment.Completed = completed.Time
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

func IsValidPaymentStatus(status string) bool {
	return status == PAYMENT_STATUS_PENDING || status == PAYMENT_STATUS_CHARGING || status == PAYMENT_STATUS_REFUNDING || status == PAYMENT_STATUS_REFUNDED || status == PAYMENT_STATUS_SUCCESS || status == PAYMENT_STATUS_ERROR || status == PAYMENT_STATUS_RETRY
}

func IsPaymentStatusCompleted(status string) bool {
	return status == PAYMENT_STATUS_REFUNDED || status == PAYMENT_STATUS_SUCCESS || status == PAYMENT_STATUS_ERROR
}

var PaymentNotFoundError = errors.New("Payment not found.")
var PaymentInvalidStatusError = errors.New("Invalid status.")
var PaymentNegativeAmountError = errors.New("Amount was negative.")

func (p *Persister) GetPayments(before, after ID, count int, status []string, user, campaign, funding_source *ID) ([]Payment, error) {
	for _, s := range status {
		if !IsValidPaymentStatus(s) {
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
	if funding_source != nil {
		keys = append(keys, "funding_source_id =")
		values = append(values, funding_source.String())
	}
	if len(status) > 0 {
		keys = append(keys, "status IN ")
		//TODO: This is HORRIBLE. These should each be separate values that are substituted individually to avoid SQL injection. This method should be used with caution and only on validated things until we get better SQL generation in place.
		values = append(values, "("+strings.Join(status, ", ")+")")
	}
	query := "SELECT * FROM payments"
	if len(keys) > 0 {
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
	id, err := p.GetID()
	if err != nil {
		return Payment{}, err
	}
	message = strings.TrimSpace(message)
	if amount < 9 {
		return Payment{}, PaymentNegativeAmountError
	}
	payment := Payment{
		ID:              id,
		Amount:          amount,
		Message:         message,
		Created:         time.Now(),
		UserID:          userID,
		FundingSourceID: fsID,
		Anonymous:       anonymous,
		Campaign:        campaignID,
		Status:          PAYMENT_STATUS_PENDING,
	}
	stmt := `INSERT INTO payments VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);`
	_, err = p.Database.Exec(stmt, payment.ID.String(), payment.RemoteID, payment.Amount, payment.Message, payment.Created, nil, payment.UserID.String(), payment.FundingSourceID.String(), payment.Anonymous, payment.Campaign, payment.Status, payment.Error)
	return payment, err
}

func (p *Persister) UpdatePayment(payment *Payment, amount *int, message *string, userID, fsID, campaignID *ID, anonymous *bool) error {
	changedKeys := []string{}
	changedValues := []interface{}{}
	if amount != nil {
		payment.Amount = *amount
		changedKeys = append(changedKeys, "amount")
		changedValues = append(changedValues, payment.Amount)
	}
	if message != nil {
		payment.Message = strings.TrimSpace(*message)
		changedKeys = append(changedKeys, "message")
		changedValues = append(changedValues, payment.Message)
	}
	if userID != nil {
		payment.UserID = *userID
		changedKeys = append(changedKeys, "user_id")
		changedValues = append(changedValues, payment.UserID.String())
	}
	if fsID != nil {
		payment.FundingSourceID = *fsID
		changedKeys = append(changedKeys, "funding_source_id")
		changedValues = append(changedValues, payment.FundingSourceID.String())
	}
	if campaignID != nil {
		payment.Campaign = *campaignID
		changedKeys = append(changedKeys, "campaign")
		changedValues = append(changedValues, payment.Campaign.String())
	}
	if anonymous != nil {
		payment.Anonymous = *anonymous
		changedKeys = append(changedKeys, "anonymous")
		changedValues = append(changedValues, payment.Anonymous)
	}
	stmt := `UPDATE payments SET`
	for index, value := range changedKeys {
		stmt += " " + value + "=$" + strconv.Itoa(index+1)
		if index+1 < len(changedKeys) {
			stmt += ","
		}
	}
	stmt += ` WHERE id=$` + strconv.Itoa(len(changedKeys)+1)
	changedValues = append(changedValues, payment.ID.String())
	_, err := p.Database.Exec(stmt, changedValues...)
	return err
}

func (p *Persister) UpdatePaymentStatus(payment *Payment, status, payment_error string) error {
	payment.Status = strings.TrimSpace(status)
	payment.Error = strings.TrimSpace(payment_error)
	if !IsValidPaymentStatus(payment.Status) {
		return PaymentInvalidStatusError
	}
	if IsPaymentStatusCompleted(status) {
		payment.Completed = time.Now()
	} else {
		payment.Completed = time.Time{}
	}
	keys := []string{"status", "error", "completed"}
	values := []interface{}{payment.Status, payment.Error, payment.Completed}
	stmt := `UPDATE payments SET`
	for index, value := range keys {
		stmt += " " + value + "=$" + strconv.Itoa(index+1)
		if index+1 < len(keys) {
			stmt += ","
		}
	}
	stmt += " WHERE id=$" + strconv.Itoa(len(keys)+1)
	values = append(values, payment.ID.String())
	_, err := p.Database.Exec(stmt, values...)
	return err
}

func (p *Persister) DeletePayment(payment Payment) error {
	stmt := `DELETE FROM payments WHERE id=$1;`
	_, err := p.Database.Exec(stmt, payment.ID.String())
	if err != nil {
		return err
	}
	return nil
}
