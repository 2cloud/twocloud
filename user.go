package twocloud

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"time"
)

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
		// TODO: report invalid auth attempt to stats
		return User{}, InvalidCredentialsError
	}
	err = p.updateUserLastActive(user.ID)
	if err != nil {
		p.Log.Error(err.Error())
	}
	// TODO: report user activity to stats
	var subscriptionError error
	if p.Config.UseSubscriptions {
		// TODO: get user subscription
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

// TODO: persist new value
func (p *Persister) updateUserLastActive(id uint64) error {
	return nil
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
	}
	// TODO: persist user
	// TODO: return UsernameTakenError on conflict: https://groups.google.com/forum/?fromgroups=#!topic/golang-nuts/TURZm6W5A5o
	p.updateSubscriptionStatus(user)
	return user, nil
}

// TODO: query for user
func (p *Persister) GetUser(id uint64) (User, error) {
	return User{}, nil
}

// TODO: query for user
func (p *Persister) GetUserByUsername(username string) (User, error) {
	return User{}, nil
}

// TODO: query for users
func (p *Persister) GetUsersByActivity(count int, after, before time.Time) ([]User, error) {
	return []User{}, nil
}

// TODO: query for users
func (p *Persister) GetUsersByJoinDate(count int, after, before time.Time) ([]User, error) {
	return []User{}, nil
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
	// TODO: persist user
	return nil
}

func (p *Persister) ResetSecret(user User) (User, error) {
	secret, err := GenerateSecret()
	if err != nil {
		return User{}, err
	}
	user.Secret = secret
	// TODO: persist new value
	return user, nil
}

func (p *Persister) VerifyEmail(user User, code string) error {
	if !user.EmailUnconfirmed {
		// TODO: return an error
	}
	if user.EmailConfirmation != code {
		return InvalidConfirmationCodeError
	}
	user.EmailUnconfirmed = false
	// TODO: persist new value
	return nil
}

func (p *Persister) MakeAdmin(user User) error {
	user.IsAdmin = true
	// TODO: persist new value
	return nil
}

func (p *Persister) StripAdmin(user User) error {
	user.IsAdmin = false
	// TODO: persist new value
	return nil
}

func (p *Persister) DeleteUser(user User) error {
	// TODO: delete the user from the repo
	// TODO: cascade that deletion to other models
	return nil
}
