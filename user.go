package twocloud

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"time"
)

var UserTableCreateStatement = `CREATE TABLE users (
	id bigint primary key,
	username varchar(20) NOT NULL UNIQUE,
	given_name varchar,
	family_name varchar,
	email varchar NOT NULL,
	email_unconfirmed bool default true,
	email_confirmation varchar NOT NULL,
	secret varchar NOT NULL,
	joined timestamp default CURRENT_TIMESTAMP,
	last_active timestamp default CURRENT_TIMESTAMP,
	is_admin bool default false,
	receive_newsletter bool default false);`

type Name struct {
	Given  string `json:"given,omitempty"`
	Family string `json:"family,omitempty"`
}

type User struct {
	ID                uint64        `json:"id,omitempty"`
	Username          string        `json:"username,omitempty"`
	Email             string        `json:"email,omitempty"`
	EmailUnconfirmed  bool          `json:"email_unconfirmed,omitempty"`
	EmailConfirmation string        `json:"-"`
	Secret            string        `json:"secret,omitempty"`
	Joined            time.Time     `json:"joined,omitempty"`
	Name              Name          `json:"name,omitempty"`
	LastActive        time.Time     `json:"last_active,omitempty"`
	IsAdmin           bool          `json:"is_admin,omitempty"`
	Subscription      *Subscription `json:"subscription,omitempty"`
	ReceiveNewsletter bool          `json:"receives_newsletter,omitempty"`
}

func (user *User) IsEmpty() bool {
	return user.ID == 0
}

func (user *User) fromRow(row ScannableRow) error {
	return row.Scan(&user.ID, &user.Username, &user.Name.Given, &user.Name.Family, &user.Email, &user.EmailUnconfirmed, &user.EmailConfirmation, &user.Secret, &user.Joined, &user.LastActive, &user.IsAdmin, &user.ReceiveNewsletter)
}

func GenerateSecret() (string, error) {
	secret := make([]byte, 64)
	_, err := io.ReadFull(rand.Reader, secret)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(secret), nil
}

func GenerateEmailConfirmation() (string, error) {
	code := make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, code)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(code), nil
}

var InvalidCredentialsError = errors.New("The credentials entered were not valid.")
var InvalidConfirmationCodeError = errors.New("The confirmation code entered was not valid.")
var UsernameTakenError = errors.New("That username is already in use. Please select another.")
var InvalidUsernameCharacterError = errors.New("An invalid character was used in the username. Only a-z, A-Z, 0-9, -, and _ are allowed in usernames.")
var InvalidUsernameLengthError = errors.New("Your username must be between 3 and 20 characters long.")
var MissingEmailError = errors.New("No email address was supplied. An email address is required.")
var UserNotFoundError = errors.New("User was not found in the database.")
var EmailAlreadyConfirmedError = errors.New("Email has already been confirmed.")

func ValidateUsername(username string) error {
	if len(username) < 3 || len(username) > 20 {
		return InvalidUsernameLengthError
	}
	asciiOnly := func(r rune) rune {
		switch {
		case r >= 'A' && r <= 'Z':
			return r
		case r >= 'a' && r <= 'z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_':
			return r
		default:
			return -1
		}
		return -1
	}
	newUsername := strings.Map(asciiOnly, username)
	if username != newUsername {
		return InvalidUsernameCharacterError
	}
	return nil
}

func (p *Persister) Authenticate(username, secret string) (User, error) {
	user, err := p.GetUserByUsername(username)
	if err != nil {
		return User{}, err
	}
	if user.Secret != secret {
		p.Log.Warn("Invalid auth attempt for %s's account.", username)
		return User{}, InvalidCredentialsError
	}
	err = p.updateUserLastActive(&user)
	if err != nil {
		p.Log.Error(err.Error())
	}
	var subscriptionError error
	if p.Config.UseSubscriptions {
		user.Subscription, err = p.GetSubscriptionByUser(user.ID)
		if err != nil {
			return User{}, err
		}
		p.updateSubscriptionStatus(user.Subscription)
		if !user.Subscription.Active && !user.IsAdmin {
			if !user.Subscription.InGracePeriod {
				subscriptionError = &SubscriptionExpiredError{Expired: user.Subscription.Expires}
			} else {
				subscriptionError = &SubscriptionExpiredWarning{Expired: user.Subscription.Expires}
			}
		}
	}
	return user, subscriptionError
}

func (p *Persister) updateUserLastActive(user *User) error {
	user.LastActive = time.Now()
	stmt := `UPDATE users SET last_active=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, user.LastActive, user.ID)
	return err
}

func (p *Persister) Register(username, email, given_name, family_name string, email_unconfirmed, is_admin, newsletter bool) (User, error) {
	email = strings.TrimSpace(email)
	username = strings.TrimSpace(username)
	given_name = strings.TrimSpace(given_name)
	family_name = strings.TrimSpace(family_name)
	err := ValidateUsername(username)
	if err != nil {
		p.Log.Error(err.Error())
		return User{}, err
	}
	if email == "" {
		return User{}, MissingEmailError
	}
	id, err := p.GetID()
	if err != nil {
		p.Log.Error(err.Error())
		return User{}, err
	}
	secret, err := GenerateSecret()
	if err != nil {
		p.Log.Error(err.Error())
		return User{}, err
	}
	code, err := GenerateEmailConfirmation()
	if err != nil {
		p.Log.Error(err.Error())
		return User{}, err
	}
	user := User{
		ID:                id,
		Username:          username,
		Email:             email,
		EmailUnconfirmed:  email_unconfirmed,
		EmailConfirmation: code,
		Secret:            secret,
		Joined:            time.Now(),
		Name: Name{
			Given:  given_name,
			Family: family_name,
		},
		LastActive: time.Now(),
		IsAdmin:    is_admin,
		Subscription: &Subscription{
			Expires: time.Now().Add(p.Config.TrialPeriod * time.Hour * 24),
		},
		ReceiveNewsletter: newsletter,
	}
	stmt := `INSERT INTO users VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);`
	_, err = p.Database.Exec(stmt, user.ID, user.Username, user.Name.Given, user.Name.Family, user.Email, user.EmailUnconfirmed, user.EmailConfirmation, user.Secret, user.Joined, user.LastActive, user.IsAdmin, user.ReceiveNewsletter)
	if err != nil {
		if isUniqueConflictError(err) {
			return User{}, UsernameTakenError
		}
		return User{}, err
	}
	p.updateSubscriptionStatus(user.Subscription)
	return user, nil
}

func (p *Persister) GetUser(id uint64) (User, error) {
	var user User
	row := p.Database.QueryRow("SELECT * FROM users WHERE id=$1", id)
	err := user.fromRow(row)
	return user, err
}

func (p *Persister) GetUserByUsername(username string) (User, error) {
	var user User
	row := p.Database.QueryRow("SELECT * FROM users WHERE username=$1", username)
	err := user.fromRow(row)
	return user, err
}

func (p *Persister) GetUsersByActivity(count int, after, before time.Time) ([]User, error) {
	users := []User{}
	var rows *sql.Rows
	var err error
	if !after.IsZero() && !before.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM users WHERE last_active > $1 and last_active < $2 ORDER BY last_active DESC LIMIT $3", after, before, count)
	} else if !after.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM users WHERE last_active > $1 ORDER BY last_active DESC LIMIT $2", after, count)
	} else if !before.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM users WHERE last_active < $1 ORDER BY last_active DESC LIMIT $2", before, count)
	} else {
		rows, err = p.Database.Query("SELECT * FROM users ORDER BY last_active DESC LIMIT $1", count)
	}
	if err != nil {
		return []User{}, err
	}
	for rows.Next() {
		var user User
		err = user.fromRow(rows)
		if err != nil {
			return []User{}, err
		}
		users = append(users, user)
	}
	err = rows.Err()
	return users, err
}

func (p *Persister) GetUsersByJoinDate(count int, after, before time.Time) ([]User, error) {
	users := []User{}
	var rows *sql.Rows
	var err error
	if !after.IsZero() && !before.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM users WHERE joined > $1 and joined < $2 ORDER BY joined DESC LIMIT $3", after, before, count)
	} else if !after.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM users WHERE joined > $1 ORDER BY joined DESC LIMIT $2", after, count)
	} else if !before.IsZero() {
		rows, err = p.Database.Query("SELECT * FROM users WHERE joined < $1 ORDER BY joined DESC LIMIT $2", before, count)
	} else {
		rows, err = p.Database.Query("SELECT * FROM users ORDER BY joined DESC LIMIT $1", count)
	}
	if err != nil {
		return []User{}, err
	}
	for rows.Next() {
		var user User
		err = user.fromRow(rows)
		if err != nil {
			return []User{}, err
		}
		users = append(users, user)
	}
	err = rows.Err()
	return users, err
}

func (p *Persister) UpdateUser(user User, email, given_name, family_name string, name_changed bool) error {
	email = strings.TrimSpace(email)
	given_name = strings.TrimSpace(given_name)
	family_name = strings.TrimSpace(family_name)
	if email != "" {
		code, err := GenerateEmailConfirmation()
		if err != nil {
			p.Log.Error(err.Error())
			return err
		}
		user.EmailConfirmation = code
		user.EmailUnconfirmed = true
		user.Email = email
	}
	if name_changed {
		user.Name.Given = given_name
		user.Name.Family = family_name
	}
	if email != "" && name_changed {
		stmt := `UPDATE users SET email=$1, email_confirmation=$2, email_unconfirmed=$3, given_name=$4, family_name=$5 WHERE id=$6;`
		_, err := p.Database.Exec(stmt, user.Email, user.EmailConfirmation, user.EmailUnconfirmed, user.Name.Given, user.Name.Family, user.ID)
		return err
	}
	if email != "" {
		stmt := `UPDATE users SET email=$1, email_confirmation=$2, email_unconfirmed=$3 WHERE id=$4;`
		_, err := p.Database.Exec(stmt, user.Email, user.EmailConfirmation, user.EmailUnconfirmed, user.ID)
		return err
	}
	if name_changed {
		stmt := `UPDATE users SET given_name=$1, family_name=$2 WHERE id=$3;`
		_, err := p.Database.Exec(stmt, user.Name.Given, user.Name.Family, user.ID)
		return err
	}
	return nil
}

func (p *Persister) ResetSecret(user *User) error {
	secret, err := GenerateSecret()
	if err != nil {
		return err
	}
	user.Secret = secret
	stmt := `UPDATE users SET secret=$1 WHERE id=$2;`
	_, err = p.Database.Exec(stmt, user.Secret, user.ID)
	return err
}

func (p *Persister) VerifyEmail(user *User, code string) error {
	if !user.EmailUnconfirmed {
		return EmailAlreadyConfirmedError
	}
	if user.EmailConfirmation != code {
		return InvalidConfirmationCodeError
	}
	user.EmailUnconfirmed = false
	stmt := `UPDATE users SET email_unconfirmed=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, false, user.ID)
	return err
}

func (p *Persister) MakeAdmin(user *User) error {
	user.IsAdmin = true
	stmt := `UPDATE users SET is_admin=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, user.IsAdmin, user.ID)
	return err
}

func (p *Persister) StripAdmin(user *User) error {
	user.IsAdmin = false
	stmt := `UPDATE users SET is_admin=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, user.IsAdmin, user.ID)
	return err
}

func (p *Persister) SubscribeToNewsletter(user *User) error {
	user.ReceiveNewsletter = true
	stmt := `UPDATE users SET receive_newsletter=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, user.ReceiveNewsletter, user.ID)
	return err
}

func (p *Persister) UnsubscribeFromNewsletter(user *User) error {
	user.ReceiveNewsletter = false
	stmt := `UPDATE users SET receive_newsletter=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, user.ReceiveNewsletter, user.ID)
	return err
}

func (p *Persister) DeleteUser(user User) error {
	stmt := `DELETE FROM users WHERE id=$1;`
	_, err := p.Database.Exec(stmt, user.ID)
	if err != nil {
		return err
	}
	// TODO: cascade that deletion to other models
	return nil
}
