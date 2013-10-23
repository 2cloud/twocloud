package twocloud

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"secondbit.org/pan"
	"strings"
	"time"
)

var UserTableCreateStatement = `CREATE TABLE users (
	id varchar primary key,
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
	Given  *string `json:"given,omitempty"`
	Family *string `json:"family,omitempty"`
}

type User struct {
	ID                ID        `json:"id,omitempty"`
	Username          string    `json:"username,omitempty"`
	Email             string    `json:"email,omitempty"`
	EmailUnconfirmed  bool      `json:"email_unconfirmed,omitempty"`
	EmailConfirmation string    `json:"-"`
	Secret            string    `json:"secret,omitempty"`
	Joined            time.Time `json:"joined,omitempty"`
	Name              Name      `json:"name,omitempty"`
	LastActive        time.Time `json:"last_active,omitempty"`
	IsAdmin           bool      `json:"is_admin,omitempty"`
	ReceiveNewsletter bool      `json:"receives_newsletter,omitempty"`
}

func (user *User) IsEmpty() bool {
	return user.ID.IsZero()
}

func (user *User) fromRow(row ScannableRow) error {
	var idStr string
	err := row.Scan(&idStr, &user.Username, &user.Name.Given, &user.Name.Family, &user.Email, &user.EmailUnconfirmed, &user.EmailConfirmation, &user.Secret, &user.Joined, &user.LastActive, &user.IsAdmin, &user.ReceiveNewsletter)
	if err != nil {
		return err
	}
	id, err := IDFromString(idStr)
	if err != nil {
		return err
	}
	user.ID = id
	return nil
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
var InvalidUsernameLengthShortError = errors.New("Your username must be between at least 3 characters long.")
var InvalidUsernameLengthLongError = errors.New("Your username must be at most 20 characters long.")
var MissingEmailError = errors.New("No email address was supplied. An email address is required.")
var UserNotFoundError = errors.New("User was not found in the database.")
var EmailAlreadyConfirmedError = errors.New("Email has already been confirmed.")

const (
	UserCreatedTopic     = "users.created"
	UserUpdatedTopic     = "users.updated"
	UserDeletedTopic     = "users.deleted"
	UserSecretResetTopic = "users.secret_reset"
)

func ValidateUsername(username string) error {
	if len(username) < 3 {
		return InvalidUsernameLengthShortError
	} else if len(username) > 20 {
		return InvalidUsernameLengthLongError
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
		if err == sql.ErrNoRows {
			err = UserNotFoundError
		}
		return User{}, err
	}
	if user.Secret != secret {
		return User{}, InvalidCredentialsError
	}
	err = p.updateUserLastActive(&user)
	if err != nil {
		return User{}, err
	}
	return user, nil
}

func (p *Persister) updateUserLastActive(user *User) error {
	user.LastActive = time.Now()
	query := pan.New()
	query.SQL = "UPDATE users SET"
	query.Include("last_active=?", user.LastActive)
	query.IncludeWhere()
	query.Include("id=?", user.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}

func (p *Persister) Register(username, email string, given_name, family_name *string, email_unconfirmed, is_admin, newsletter bool) (User, error) {
	email = strings.TrimSpace(email)
	username = strings.TrimSpace(username)
	if given_name != nil {
		tmpGivenName := strings.TrimSpace(*given_name)
		given_name = &tmpGivenName
	}
	if family_name != nil {
		tmpFamilyName := strings.TrimSpace(*family_name)
		family_name = &tmpFamilyName
	}
	err := ValidateUsername(username)
	if err != nil {
		return User{}, err
	}
	if email == "" {
		return User{}, MissingEmailError
	}
	id, err := p.GetID()
	if err != nil {
		return User{}, err
	}
	secret, err := GenerateSecret()
	if err != nil {
		return User{}, err
	}
	code, err := GenerateEmailConfirmation()
	if err != nil {
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
		LastActive:        time.Now(),
		IsAdmin:           is_admin,
		ReceiveNewsletter: newsletter,
	}
	query := pan.New()
	query.SQL = "INSERT INTO users VALUES("
	query.Include("?", user.ID.String())
	query.Include("?", user.Username)
	query.Include("?", user.Name.Given)
	query.Include("?", user.Name.Family)
	query.Include("?", user.Email)
	query.Include("?", user.EmailUnconfirmed)
	query.Include("?", user.EmailConfirmation)
	query.Include("?", user.Secret)
	query.Include("?", user.Joined)
	query.Include("?", user.LastActive)
	query.Include("?", user.IsAdmin)
	query.Include("?", user.ReceiveNewsletter)
	query.FlushExpressions(", ")
	query.SQL += ")"
	_, err = p.Database.Exec(query.Generate(" "), query.Args...)
	if err != nil {
		if isUniqueConflictError(err) {
			return User{}, UsernameTakenError
		}
		return User{}, err
	}
	_, nsqErr := p.Publish(UserCreatedTopic, []byte(user.ID.String()))
	if nsqErr != nil {
		p.Log.Error(nsqErr.Error())
	}
	return user, nil
}

func (p *Persister) GetUser(id ID) (User, error) {
	var user User
	query := pan.New()
	query.SQL = "SELECT * FROM users"
	query.IncludeWhere()
	query.Include("id=?", id.String())
	row := p.Database.QueryRow(query.Generate(" "), query.Args...)
	err := user.fromRow(row)
	return user, err
}

func (p *Persister) GetUserByUsername(username string) (User, error) {
	var user User
	query := pan.New()
	query.SQL = "SELECT * FROM users"
	query.IncludeWhere()
	query.Include("LOWER(username)=LOWER(?)", username)
	row := p.Database.QueryRow(query.Generate(" "), query.Args...)
	err := user.fromRow(row)
	return user, err
}

func (p *Persister) GetUsersByActivity(count int, after, before time.Time) ([]User, error) {
	users := []User{}
	query := pan.New()
	query.SQL = "SELECT * FROM users"
	if !after.IsZero() {
		query.IncludeWhere()
		query.Include("last_active > ?", after)
	}
	if !before.IsZero() {
		query.IncludeWhere()
		query.Include("last_active < ?", before)
	}
	query.FlushExpressions(" and ")
	query.IncludeOrder("last_active DESC")
	query.IncludeLimit(count)
	rows, err := p.Database.Query(query.Generate(" "), query.Args...)
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
	query := pan.New()
	query.SQL = "SELECT * FROM users"
	if !after.IsZero() {
		query.IncludeWhere()
		query.Include("joined > ?", after)
	}
	if !before.IsZero() {
		query.IncludeWhere()
		query.Include("joined < ?", before)
	}
	query.FlushExpressions(" and ")
	query.IncludeOrder("joined DESC")
	query.IncludeLimit(count)
	rows, err := p.Database.Query(query.Generate(" "), query.Args...)
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

func (p *Persister) UpdateUser(user *User, email, given_name, family_name *string, newsletter *bool) error {
	query := pan.New()
	query.SQL = "UPDATE users SET "
	if email != nil {
		newEmail := strings.TrimSpace(*email)
		if newEmail != "" {
			code, err := GenerateEmailConfirmation()
			if err != nil {
				return err
			}
			user.EmailConfirmation = code
			user.EmailUnconfirmed = true
			user.Email = newEmail
			query.Include("email=?", user.Email)
			query.Include("email_confirmation=?", user.EmailConfirmation)
			query.Include("email_unconfirmed=?", user.EmailUnconfirmed)
		}
	}
	if given_name != nil {
		tmpGivenName := strings.TrimSpace(*given_name)
		user.Name.Given = &tmpGivenName
		query.Include("given_name=?", user.Name.Given)
	}
	if family_name != nil {
		tmpFamilyName := strings.TrimSpace(*family_name)
		user.Name.Family = &tmpFamilyName
		query.Include("family_name=?", user.Name.Family)
	}
	if newsletter != nil {
		user.ReceiveNewsletter = *newsletter
		query.Include("receive_newsletter=?", user.ReceiveNewsletter)
	}
	query.FlushExpressions(", ")
	query.IncludeWhere()
	query.Include("id=?", user.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err == nil {
		_, nsqErr := p.Publish(UserUpdatedTopic, []byte(user.ID.String()))
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return err
}

func (p *Persister) ResetSecret(user *User) error {
	secret, err := GenerateSecret()
	if err != nil {
		return err
	}
	user.Secret = secret
	query := pan.New()
	query.SQL = "UPDATE users SET"
	query.Include("secret=?", user.Secret)
	query.IncludeWhere()
	query.Include("id=?", user.ID.String())
	_, err = p.Database.Exec(query.Generate(" "), query.Args...)
	if err == nil {
		_, nsqErr := p.Publish(UserSecretResetTopic, []byte(user.ID.String()))
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
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
	query := pan.New()
	query.SQL = "UPDATE users SET"
	query.Include("email_unconfirmed=?", user.EmailUnconfirmed)
	query.IncludeWhere()
	query.Include("id=?", user.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err == nil {
		_, nsqErr := p.Publish(UserUpdatedTopic, []byte(user.ID.String()))
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return err
}

func (p *Persister) MakeAdmin(user *User) error {
	user.IsAdmin = true
	query := pan.New()
	query.SQL = "UPDATE users SET"
	query.Include("is_admin=?", user.IsAdmin)
	query.IncludeWhere()
	query.Include("id=?", user.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err == nil {
		_, nsqErr := p.Publish(UserUpdatedTopic, []byte(user.ID.String()))
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return err
}

func (p *Persister) StripAdmin(user *User) error {
	user.IsAdmin = false
	query := pan.New()
	query.SQL = "UPDATE users SET"
	query.Include("is_admin=?", user.IsAdmin)
	query.IncludeWhere()
	query.Include("id=?", user.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err == nil {
		_, nsqErr := p.Publish(UserUpdatedTopic, []byte(user.ID.String()))
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return err
}

func (p *Persister) SubscribeToNewsletter(user *User) error {
	user.ReceiveNewsletter = true
	query := pan.New()
	query.SQL = "UPDATE users SET"
	query.Include("receive_newsletter=?", user.ReceiveNewsletter)
	query.IncludeWhere()
	query.Include("id=?", user.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err == nil {
		_, nsqErr := p.Publish(UserUpdatedTopic, []byte(user.ID.String()))
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return err
}

func (p *Persister) UnsubscribeFromNewsletter(user *User) error {
	user.ReceiveNewsletter = false
	query := pan.New()
	query.SQL = "UPDATE users SET"
	query.Include("receive_newsletter=?", user.ReceiveNewsletter)
	query.IncludeWhere()
	query.Include("id=?", user.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err == nil {
		_, nsqErr := p.Publish(UserUpdatedTopic, []byte(user.ID.String()))
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return err
}

func (p *Persister) DeleteUsers(users []User, cascade bool) error {
	query := pan.New()
	query.SQL = "DELETE FROM users"
	query.IncludeWhere()
	queryKeys := make([]string, len(users))
	queryVals := make([]interface{}, len(users))
	for _, user := range users {
		queryKeys = append(queryKeys, "?")
		queryVals = append(queryVals, user.ID.String())
	}
	query.Include("id IN("+strings.Join(queryKeys, ", ")+")", queryVals...)
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err != nil {
		return err
	}
	for _, user := range users {
		_, nsqErr := p.Publish(UserDeletedTopic, []byte(user.ID.String()))
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	if cascade {
		err = p.DeleteAccountsByUsers(users)
		if err != nil {
			return err
		}
		err = p.DeleteDevicesByUsers(users, true)
		if err != nil {
			return err
		}
		err = p.DeleteStripeSourcesByUsers(users)
		if err != nil {
			return err
		}
		err = p.CancelSubscriptionsByUsers(users)
		if err != nil {
			return err
		}
		err = p.AnonymizePaymentsByUsers(users)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Persister) DeleteUser(user User, cascade bool) error {
	return p.DeleteUsers([]User{user}, cascade)
}
