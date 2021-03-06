package twocloud

import (
	crypto "crypto/rand"
	"encoding/hex"
	"errors"
	"github.com/fzzbt/radix/redis"
	"io"
	"math/rand"
	"strconv"
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
}

type Subscription struct {
	ID            string    `json:"id,omitempty"`
	Active        bool      `json:"active,omitempty"`
	InGracePeriod bool      `json:"in_grace_period,omitempty"`
	Expires       time.Time `json:"expires,omitempty"`
	AuthTokens    []string  `json:"auth_tokens,omitempty"`
}

func GenerateTempCredentials() string {
	cred := ""
	acceptableChars := [50]string{"a", "b", "c", "d", "e", "f", "g", "h", "j", "k", "m", "n", "p", "q", "r", "s", "t", "w", "x", "y", "z", "A", "B", "C", "D", "E", "F", "G", "H", "J", "K", "M", "N", "P", "Q", "R", "S", "T", "W", "X", "Y", "Z", "2", "3", "4", "5", "6", "7", "8", "9"}
	for i := 0; i < 5; i++ {
		rand.Seed(time.Now().UnixNano())
		cred = cred + acceptableChars[rand.Intn(50)]
	}
	return cred
}

func GenerateSecret() (string, error) {
	secret := make([]byte, 64)
	_, err := io.ReadFull(crypto.Reader, secret)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(secret), nil
}

func GenerateEmailConfirmation() (string, error) {
	code := make([]byte, 32)
	_, err := io.ReadFull(crypto.Reader, code)
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

func (r *RequestBundle) Authenticate(username, secret string) (User, error) {
	// start instrumentation
	id, err := r.GetUserID(username)
	if err != nil {
		return User{}, err
	}
	// add cache/repo calls to instrumentation
	user, err := r.GetUser(id)
	if err != nil {
		return User{}, err
	}
	// add repo calls to instrumentation
	err = r.updateAuthErrorFlag(user.Secret != secret)
	// add repo call to instrumentation
	if err != nil {
		return User{}, err
	}
	if user.Secret != secret {
		r.Log.Warn("Invalid auth attempt for %s's account.", username)
		// report invalid auth attempt to stats
		return User{}, InvalidCredentialsError
	}
	err = r.updateUserLastActive(id)
	if err != nil {
		r.Log.Error(err.Error())
	}
	// add repo call to instrumentation
	// report user activity to stats
	// add repo calls to instrumentation
	var subscriptionError error
	if r.Config.UseSubscriptions {
		r.UpdateSubscriptionStatus(user)
		if !user.Subscription.Active && !user.IsAdmin {
			if !user.Subscription.InGracePeriod {
				subscriptionError = &SubscriptionExpiredError{Expired: user.Subscription.Expires}
			} else {
				subscriptionError = &SubscriptionExpiredWarning{Expired: user.Subscription.Expires}
			}
		}
	}
	// store instrumentation
	return user, subscriptionError
}

func (r *RequestBundle) updateUserLastActive(id uint64) error {
	// start instrumentation
	reply := r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		mc.Hset("users:"+strconv.FormatUint(id, 10), "last_active", time.Now().Format(time.RFC3339))
		mc.Zadd("users_by_last_active", time.Now().Unix(), id)
	})
	// report repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return reply.Err
	}
	// stop instrumentation
	return nil
}

func (r *RequestBundle) Register(username, email, given_name, family_name string, email_unconfirmed, is_admin bool) (User, error) {
	// start instrumentation
	email = strings.TrimSpace(email)
	username = strings.TrimSpace(username)
	given_name = strings.TrimSpace(given_name)
	family_name = strings.TrimSpace(family_name)
	err := ValidateUsername(username)
	if err != nil {
		r.Log.Error(err.Error())
		return User{}, err
	}
	if email == "" {
		return User{}, MissingEmailError
	}
	id, err := r.GetID()
	if err != nil {
		r.Log.Error(err.Error())
		return User{}, err
	}
	secret, err := GenerateSecret()
	if err != nil {
		r.Log.Error(err.Error())
		return User{}, err
	}
	code, err := GenerateEmailConfirmation()
	if err != nil {
		r.Log.Error(err.Error())
		return User{}, err
	}
	success, err := r.reserveUsername(username, id)
	// add repo call to instrumentation
	if err != nil {
		return User{}, err
	}
	if !success {
		return User{}, UsernameTakenError
	}
	// add repo calls to instrumentation
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
			Expires: time.Now().Add(r.Config.TrialPeriod * time.Hour * 24),
		},
	}
	err = r.storeUser(user, false)
	// add repo calls to instrumentation
	if err != nil {
		release_err := r.releaseUsername(username)
		if release_err != nil {
			r.Log.Error(release_err.Error())
		}
		// add repo call to instrumentation
		r.Log.Error(err.Error())
		return User{}, err
	}
	r.UpdateSubscriptionStatus(user)
	// log the user registration in stats
	// add repo calls to instrumentation
	// send the confirmation email
	// stop instrumentation
	return user, nil
}

func (r *RequestBundle) reserveUsername(username string, id uint64) (bool, error) {
	// start instrumentation
	reply := r.Repo.client.Hsetnx("usernames_to_ids", strings.ToLower(username), id)
	// report repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return false, reply.Err
	}
	r.Audit("usernames_to_ids", strings.ToLower(username), "", strconv.FormatUint(id, 10))
	// report repo calls to instrumentation
	// stop instrumentation
	return reply.Bool()
}

func (r *RequestBundle) releaseUsername(username string) error {
	// start instrumentation
	reply := r.Repo.client.Hget("usernames_to_ids", strings.ToLower(username))
	// report the repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return reply.Err
	}
	if reply.Type == redis.ReplyNil {
		return nil
	}
	was, err := reply.Str()
	if err != nil {
		r.Log.Error(err.Error())
		return err
	}
	reply = r.Repo.client.Hdel("usernames_to_ids", strings.ToLower(username))
	// report the repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(err.Error())
		return reply.Err
	}
	r.Audit("usernames_to_ids", strings.ToLower(username), was, "")
	// report repo calls to instrumentation
	// stop instrumentation
	return nil
}

func (r *RequestBundle) storeUser(user User, update bool) error {
	// start instrumentation
	if update {
		changes := map[string]interface{}{}
		from := map[string]interface{}{}
		old_user, err := r.GetUser(user.ID)
		// add repo call to instrumentation
		if err != nil {
			return err
		}
		if old_user.Email != user.Email {
			changes["email"] = user.Email
			from["email"] = old_user.Email
		}
		if old_user.EmailConfirmation != user.EmailConfirmation {
			changes["email_confirmation"] = user.EmailConfirmation
			from["email_confirmation"] = old_user.EmailConfirmation
		}
		if old_user.EmailUnconfirmed != user.EmailUnconfirmed {
			changes["email_unconfirmed"] = user.EmailUnconfirmed
			from["email_unconfirmed"] = old_user.EmailUnconfirmed
		}
		if old_user.IsAdmin != user.IsAdmin {
			changes["is_admin"] = user.IsAdmin
			from["is_admin"] = old_user.IsAdmin
		}
		if old_user.Name.Family != user.Name.Family {
			changes["family_name"] = user.Name.Family
			from["family_name"] = old_user.Name.Family
		}
		if old_user.Name.Given != user.Name.Given {
			changes["given_name"] = user.Name.Given
			from["given_name"] = old_user.Name.Given
		}
		reply := r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
			mc.Hmset("users:"+strconv.FormatUint(user.ID, 10), changes)
		})
		// add repo call to instrumentation
		if reply.Err != nil {
			r.Log.Error(reply.Err.Error())
			return reply.Err
		}
		r.AuditMap("users:"+strconv.FormatUint(user.ID, 10), from, changes)
		// add repo call to instrumentation
		return nil
	}
	changes := map[string]interface{}{
		"username":             user.Username,
		"email":                user.Email,
		"email_unconfirmed":    user.EmailUnconfirmed,
		"email_confirmation":   user.EmailConfirmation,
		"secret":               user.Secret,
		"joined":               user.Joined.Format(time.RFC3339),
		"given_name":           user.Name.Given,
		"family_name":          user.Name.Family,
		"last_active":          user.LastActive.Format(time.RFC3339),
		"is_admin":             user.IsAdmin,
		"subscription_id":      user.Subscription.ID,
		"subscription_expires": user.Subscription.Expires.Format(time.RFC3339),
	}
	from := map[string]interface{}{
		"username":             "",
		"email":                "",
		"email_unconfirmed":    "",
		"email_confirmation":   "",
		"secret":               "",
		"joined":               "",
		"given_name":           "",
		"family_name":          "",
		"last_active":          "",
		"is_admin":             "",
		"subscription_id":      "",
		"subscription_expires": "",
	}
	reply := r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		mc.Hmset("users:"+strconv.FormatUint(user.ID, 10), changes)
		mc.Zadd("users_by_join_date", user.Joined.Unix(), user.ID)
		mc.Zadd("users_by_subscription_expiration", user.Subscription.Expires.Unix(), user.ID)
	})
	// add repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return reply.Err
	}
	r.AuditMap("users:"+strconv.FormatUint(user.ID, 10), from, changes)
	// add repo call to instrumentation
	// stop instrumentation
	return nil
}

func (r *RequestBundle) GetUser(id uint64) (User, error) {
	// start instrumentation
	reply := r.Repo.client.Hgetall("users:" + strconv.FormatUint(id, 10))
	// add repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return User{}, reply.Err
	}
	if reply.Type == redis.ReplyNil {
		return User{}, UserNotFoundError
	}
	hash, err := reply.Hash()
	if err != nil {
		r.Log.Error(err.Error())
		return User{}, err
	}
	joined, err := time.Parse(time.RFC3339, hash["joined"])
	if err != nil {
		r.Log.Error(err.Error())
		return User{}, err
	}
	last_active, err := time.Parse(time.RFC3339, hash["last_active"])
	if err != nil {
		r.Log.Error(err.Error())
		return User{}, err
	}
	subscription_expires, err := time.Parse(time.RFC3339, hash["subscription_expires"])
	if err != nil {
		r.Log.Error(err.Error())
		return User{}, err
	}
	user := User{
		ID:                id,
		Username:          hash["username"],
		Email:             hash["email"],
		EmailUnconfirmed:  hash["email_unconfirmed"] == "1",
		EmailConfirmation: hash["email_confirmation"],
		Secret:            hash["secret"],
		Joined:            joined,
		Name: Name{
			Given:  hash["given_name"],
			Family: hash["family_name"],
		},
		LastActive: last_active,
		IsAdmin:    hash["is_admin"] == "1",
		Subscription: &Subscription{
			Expires: subscription_expires,
			ID:      hash["subscription_id"],
		},
	}
	r.UpdateSubscriptionStatus(user)
	// stop instrumentation
	return user, nil
}

func (r *RequestBundle) GetUserID(username string) (uint64, error) {
	var idstr string
	var err error
	// start instrumentation
	// check cache for user id
	// add cache check to instrumentation
	// if cached, return id
	reply := r.Repo.client.Hget("usernames_to_ids", strings.ToLower(username))
	// add repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return uint64(0), reply.Err
	}
	if reply.Type == redis.ReplyNil {
		return uint64(0), UserNotFoundError
	}
	idstr, err = reply.Str()
	if err != nil {
		r.Log.Error(err.Error())
		return uint64(0), err
	}
	// cache the user id
	// add cache request to instrumentation
	id, err := strconv.ParseUint(idstr, 10, 64)
	if err != nil {
		r.Log.Error(err.Error())
		return uint64(0), err
	}
	// stop instrumentation
	return id, nil
}

func (r *RequestBundle) GetUsersByActivity(count int, active_after, active_before time.Time) ([]User, error) {
	// start instrumentation
	var reply *redis.Reply
	var list []string
	var err error
	if !active_after.IsZero() && !active_before.IsZero() {
		reply = r.Repo.client.Zrevrangebyscore("users_by_last_active", active_before.Unix(), active_after.Unix())
		if reply.Err != nil {
			r.Log.Error(reply.Err.Error())
			return []User{}, reply.Err
		}
		list, err = reply.List()
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
	} else if !active_after.IsZero() && active_before.IsZero() {
		reply = r.Repo.client.Zrangebyscore("users_by_last_active", active_before.Unix(), time.Now().Unix())
		if reply.Err != nil {
			r.Log.Error(reply.Err.Error())
			return []User{}, reply.Err
		}
		list, err = reply.List()
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
		for i, j := 0, len(list)-1; i < j; i, j = i+1, j-1 {
			list[i], list[j] = list[j], list[i]
		}
	} else if active_after.IsZero() && !active_before.IsZero() {
		reply = r.Repo.client.Zrevrangebyscore("users_by_last_active", active_before.Unix(), active_after.Unix())
		if reply.Err != nil {
			r.Log.Error(reply.Err.Error())
			return []User{}, reply.Err
		}
		list, err = reply.List()
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
	} else {
		reply = r.Repo.client.Zrevrangebyscore("users_by_last_active", time.Now().Unix(), active_after.Unix())
		if reply.Err != nil {
			r.Log.Error(reply.Err.Error())
			return []User{}, reply.Err
		}
		list, err = reply.List()
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
	}
	reply = r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		for pos, id := range list {
			if pos >= count {
				break
			}
			mc.Hgetall("users:" + id)
		}
	})
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return []User{}, reply.Err
	}
	var users []User
	for pos, elem := range reply.Elems {
		hash, err := elem.Hash()
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
		joined, err := time.Parse(time.RFC3339, hash["joined"])
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
		last_active, err := time.Parse(time.RFC3339, hash["last_active"])
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
		subscription_expires, err := time.Parse(time.RFC3339, hash["subscription_expires"])
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
		id, err := strconv.ParseUint(list[pos], 10, 64)
		if err != nil {
			r.Log.Error(err.Error())
			continue
		}
		user := User{
			ID:                id,
			Username:          hash["username"],
			Email:             hash["email"],
			EmailUnconfirmed:  hash["email_unconfirmed"] == "1",
			EmailConfirmation: hash["email_confirmation"],
			Secret:            hash["secret"],
			Joined:            joined,
			Name: Name{
				Given:  hash["given_name"],
				Family: hash["family_name"],
			},
			LastActive: last_active,
			IsAdmin:    hash["is_admin"] == "1",
			Subscription: &Subscription{
				Expires: subscription_expires,
				ID:      hash["subscription_id"],
			},
		}
		r.UpdateSubscriptionStatus(user)
		users = append(users, user)
	}
	// stop instrumentation
	return users, nil
}

func (r *RequestBundle) GetUsersByJoinDate(count int, after, before time.Time) ([]User, error) {
	// start instrumentation
	var reply *redis.Reply
	var list []string
	var err error
	if !after.IsZero() && !before.IsZero() {
		reply = r.Repo.client.Zrevrangebyscore("users_by_join_date", before.Unix(), after.Unix())
		if reply.Err != nil {
			r.Log.Error(reply.Err.Error())
			return []User{}, reply.Err
		}
		list, err = reply.List()
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
	} else if !after.IsZero() && before.IsZero() {
		reply = r.Repo.client.Zrangebyscore("users_by_join_date", before.Unix(), time.Now().Unix())
		if reply.Err != nil {
			r.Log.Error(reply.Err.Error())
			return []User{}, reply.Err
		}
		list, err = reply.List()
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
		for i, j := 0, len(list)-1; i < j; i, j = i+1, j-1 {
			list[i], list[j] = list[j], list[i]
		}
	} else if after.IsZero() && !before.IsZero() {
		reply = r.Repo.client.Zrevrangebyscore("users_by_join_date", before.Unix(), after.Unix())
		if reply.Err != nil {
			r.Log.Error(reply.Err.Error())
			return []User{}, reply.Err
		}
		list, err = reply.List()
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
	} else {
		reply = r.Repo.client.Zrevrangebyscore("users_by_join_date", time.Now().Unix(), after.Unix())
		if reply.Err != nil {
			r.Log.Error(reply.Err.Error())
			return []User{}, reply.Err
		}
		list, err = reply.List()
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
	}
	reply = r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		for pos, id := range list {
			if pos >= count {
				break
			}
			mc.Hgetall("users:" + id)
		}
	})
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return []User{}, reply.Err
	}
	var users []User
	for pos, elem := range reply.Elems {
		hash, err := elem.Hash()
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
		joined, err := time.Parse(time.RFC3339, hash["joined"])
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
		last_active, err := time.Parse(time.RFC3339, hash["last_active"])
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
		subscription_expires, err := time.Parse(time.RFC3339, hash["subscription_expires"])
		if err != nil {
			r.Log.Error(err.Error())
			return []User{}, err
		}
		id, err := strconv.ParseUint(list[pos], 10, 64)
		if err != nil {
			r.Log.Error(err.Error())
			continue
		}
		user := User{
			ID:                id,
			Username:          hash["username"],
			Email:             hash["email"],
			EmailUnconfirmed:  hash["email_unconfirmed"] == "1",
			EmailConfirmation: hash["email_confirmation"],
			Secret:            hash["secret"],
			Joined:            joined,
			Name: Name{
				Given:  hash["given_name"],
				Family: hash["family_name"],
			},
			LastActive: last_active,
			IsAdmin:    hash["is_admin"] == "1",
			Subscription: &Subscription{
				Expires: subscription_expires,
				ID:      hash["subscription_id"],
			},
		}
		r.UpdateSubscriptionStatus(user)
		users = append(users, user)
	}
	// stop instrumentation
	return users, nil
}

func (r *RequestBundle) UpdateUser(user User, email, given_name, family_name string, name_changed bool) error {
	// start instrumentation
	email = strings.TrimSpace(email)
	given_name = strings.TrimSpace(given_name)
	family_name = strings.TrimSpace(family_name)
	email_changed := false
	if email != "" {
		code, err := GenerateEmailConfirmation()
		if err != nil {
			r.Log.Error(err.Error())
			return err
		}
		user.EmailConfirmation = code
		user.EmailUnconfirmed = true
		user.Email = email
		email_changed = true
	}
	if name_changed {
		user.Name.Given = given_name
		user.Name.Family = family_name
	}
	err := r.storeUser(user, true)
	// add repo request to instrumentation
	if err != nil {
		return err
	}
	if email_changed {
		// send the confirmation email
	}
	// send the push notification
	// stop the instrumentation
	return nil
}

func (r *RequestBundle) ResetSecret(user User) (User, error) {
	// start instrumentation
	secret, err := GenerateSecret()
	if err != nil {
		return User{}, err
	}
	user.Secret = secret
	err = r.storeUser(user, true)
	// add the repo request to instrumentation
	if err != nil {
		return User{}, err
	}
	// stop instrumentation
	return user, nil
}

func (r *RequestBundle) VerifyEmail(user User, code string) error {
	// start instrumentation
	if !user.EmailUnconfirmed {
		// return an error
	}
	if user.EmailConfirmation != code {
		return InvalidConfirmationCodeError
	}
	user.EmailUnconfirmed = false
	err := r.storeUser(user, true)
	// add the repo request to instrumentation
	if err != nil {
		return err
	}
	// log the verified email in stats
	// send the push notification
	// stop instrumentation
	return nil
}

func (r *RequestBundle) MakeAdmin(user User) error {
	// start instrumentation
	user.IsAdmin = true
	err := r.storeUser(user, true)
	// add the repo request to instrumentation
	if err != nil {
		return err
	}
	// send the push notification
	// stop instrumentation
	return nil
}

func (r *RequestBundle) StripAdmin(user User) error {
	// start instrumentation
	user.IsAdmin = false
	err := r.storeUser(user, true)
	// add the repo request to instrumentation
	if err != nil {
		return err
	}
	// send the push notification
	// stop instrumentation
	return nil
}

func (r *RequestBundle) CreateTempCredentials(user User) ([2]string, error) {
	// start instrumentation
	tmpcred1 := GenerateTempCredentials()
	tmpcred2 := GenerateTempCredentials()
	cred1 := tmpcred1
	cred2 := tmpcred2
	if tmpcred1 > tmpcred2 {
		cred1 = tmpcred2
		cred2 = tmpcred1
	}
	reply := r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		mc.Set("tokens:"+cred1+":"+cred2, user.ID)
		mc.Expire("tokens:"+cred1+":"+cred2, "300")
	})
	// add the repo request to instrumentation
	if reply.Err != nil {
		return [2]string{"", ""}, reply.Err
	}
	for _, rep := range reply.Elems {
		if rep.Err != nil {
			return [2]string{"", ""}, rep.Err
		}
	}
	r.Audit("tokens:"+strconv.FormatUint(user.ID, 10), cred1, "", cred2)
	// add the repo requests to instrumentation
	return [2]string{cred1, cred2}, nil
}

func (r *RequestBundle) CheckTempCredentials(cred1, cred2 string) (uint64, error) {
	// start instrumentation
	firstcred := cred1
	secondcred := cred2
	if firstcred > secondcred {
		firstcred = cred2
		secondcred = cred1
	}
	reply := r.Repo.client.Get("tokens:" + firstcred + ":" + secondcred)
	// add the repo request to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return uint64(0), reply.Err
	}
	if reply.Type == redis.ReplyNil {
		// add invalid credential error to stats
		// add the repo requests to instrumentation
		return uint64(0), InvalidCredentialsError
	}
	val, err := reply.Str()
	if err != nil {
		r.Log.Error(err.Error())
		return uint64(0), err
	}
	id, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		r.Log.Error(err.Error())
		return uint64(0), err
	}
	return id, nil
	// stop instrumentation
}

func (r *RequestBundle) DeleteUser(user User) error {
	// start instrumentation
	// delete the user from the repo
	// add the repo request to instrumentation
	// clear the username from cache
	// add the cache request to instrumentation
	// log the deletion in the audit log
	// add the repo requests to instrumentation
	// cascade that deletion to other models
	// add the repo requests to instrumentation
	// log the deletion in stats
	// add the repo requests to instrumentation
	// send the push notification
	// stop instrumentation
	return nil
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
