package twocloud

import (
	"database/sql"
	"errors"
	"github.com/lib/pq"
	"secondbit.org/pan"
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
	query := pan.New()
	query.SQL = "SELECT * FROM payments"
	if !before.IsZero() {
		query.IncludeWhere()
		query.Include("id < ?", before.String())
	}
	if !after.IsZero() {
		query.IncludeWhere()
		query.Include("id > ?", after.String())
	}
	if user != nil {
		query.IncludeWhere()
		query.Include("user_id=?", user.String())
	}
	if campaign != nil {
		query.IncludeWhere()
		query.Include("campaign=?", campaign.String())
	}
	if funding_source != nil {
		query.IncludeWhere()
		query.Include("funding_source_id=?", funding_source.String())
	}
	if len(status) > 0 {
		statusKeys := make([]string, len(status))
		statuses := make([]interface{}, len(status))
		for i, v := range status {
			statusKeys[i] = "?"
			statuses[i] = v
		}
		query.Include("("+strings.Join(statusKeys, ", ")+")", statuses...)
	}
	query.FlushExpressions(" and ")
	query.IncludeOrder("created DESC")
	query.IncludeLimit(count)
	rows, err := p.Database.Query(query.Generate(" "), query.Args...)
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
	query := pan.New()
	query.SQL = "SELECT * FROM payments"
	query.IncludeWhere()
	query.Include("id=?", id.String())
	row := p.Database.QueryRow(query.Generate(" "), query.Args...)
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
	query := pan.New()
	query.SQL = "INSERT INTO payments VALUES("
	query.Include("?", payment.ID.String())
	query.Include("?", payment.RemoteID)
	query.Include("?", payment.Amount)
	query.Include("?", payment.Message)
	query.Include("?", payment.Created)
	query.Include("?", nil)
	query.Include("?", payment.UserID.String())
	query.Include("?", payment.FundingSourceID.String())
	query.Include("?", payment.Anonymous)
	query.Include("?", payment.Campaign)
	query.Include("?", payment.Status)
	query.Include("?", payment.Error)
	query.FlushExpressions(", ")
	query.SQL += ")"
	_, err = p.Database.Exec(query.Generate(" "), query.Args...)
	return payment, err
}

func (p *Persister) UpdatePayment(payment *Payment, amount *int, message *string, userID, fsID, campaignID *ID, anonymous *bool) error {
	query := pan.New()
	query.SQL = "UPDATE payments SET"
	if amount != nil {
		payment.Amount = *amount
		query.Include("amount=?", payment.Amount)
	}
	if message != nil {
		payment.Message = strings.TrimSpace(*message)
		query.Include("message=?", payment.Message)
	}
	if userID != nil {
		payment.UserID = *userID
		query.Include("user_id=?", payment.UserID.String())
	}
	if fsID != nil {
		payment.FundingSourceID = *fsID
		query.Include("funding_source_id=?", payment.FundingSourceID.String())
	}
	if campaignID != nil {
		payment.Campaign = *campaignID
		query.Include("campaign=?", payment.Campaign.String())
	}
	if anonymous != nil {
		payment.Anonymous = *anonymous
		query.Include("anonymous=?", payment.Anonymous)
	}
	query.FlushExpressions(", ")
	query.IncludeWhere()
	query.Include("id=?", payment.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
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
	query := pan.New()
	query.SQL = "UPDATE payments SET"
	query.Include("status=?", payment.Status)
	query.Include("error=?", payment.Error)
	query.Include("completed=?", payment.Completed)
	query.FlushExpressions(", ")
	query.IncludeWhere()
	query.Include("id=?", payment.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}

func (p *Persister) DeletePayment(payment Payment) error {
	query := pan.New()
	query.SQL = "DELETE FROM payments"
	query.Include("id=?", payment.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err != nil {
		return err
	}
	return nil
}
