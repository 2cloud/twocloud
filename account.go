package twocloud

import (
	"code.google.com/p/goauth2/oauth"
	"encoding/json"
	"errors"
	"github.com/fzzbt/radix/redis"
	"io/ioutil"
	"net/http"
	"secondbit.org/ruid"
	"time"
)

type Account struct {
	Added    time.Time `json:"added,omitempty"`
	ID       ruid.RUID `json:"id,omitempty"`
	Provider string    `json:"provider,omitempty"`
	// Provided by the provider
	ForeignID     string `json:"foreign_id,omitempty"`
	Email         string `json:"email,omitempty"`
	EmailVerified bool   `json:"email_verified,omitempty"`
	DisplayName   string `json:"display_name,omitempty"`
	GivenName     string `json:"given_name,omitempty"`
	FamilyName    string `json:"family_name,omitempty"`
	Picture       string `json:"picture,omitempty"`
	Locale        string `json:"locale,omitempty"`
	Timezone      string `json:"timezone,omitempty"`
	Gender        string `json:"gender,omitempty"`
	// private info that is stored, never shared
	UserID       ruid.RUID `json:"-"`
	accessToken  string
	refreshToken string
	expires      time.Time
}

func (account *Account) IsEmpty() bool {
	return account.ID.String() == ruid.RUID(0).String()
}

type googleAccount struct {
	ID            string       `json:"id,omitempty"`
	Email         string       `json:"email,omitempty"`
	VerifiedEmail bool         `json:"verified_email,omitempty"`
	Name          string       `json:"name,omitempty"`
	GivenName     string       `json:"given_name,omitempty"`
	FamilyName    string       `json:"family_name,omitempty"`
	Picture       string       `json:"picture,omitempty"`
	Locale        string       `json:"locale,omitempty"`
	Timezone      string       `json:"timezone,omitempty"`
	Gender        string       `json:"gender,omitempty"`
	Error         *googleError `json:"error,omitempty"`
}

type googleError struct {
	StatusCode int    `json:"code,omitempty"`
	Message    string `json:"message,omitempty"`
}

type OAuthError string

func (err OAuthError) Error() string {
	return string(err)
}

var OAuthAuthError = errors.New("Invalid OAuth credentials.")

func (r *RequestBundle) GetOAuthAuthURL(client_id, client_secret, callback_url, state string) string {
	// start instrumentation
	config := &oauth.Config{
		ClientId:     client_id,
		ClientSecret: client_secret,
		Scope:        "https://www.googleapis.com/auth/userinfo.profile https://www.googleapis.com/auth/userinfo.email",
		AuthURL:      "https://accounts.google.com/o/oauth2/auth",
		TokenURL:     "https://accounts.google.com/o/oauth2/token",
		RedirectURL:  callback_url,
	}
	// stop instrumentation
	return config.AuthCodeURL(state)
}

func (r *RequestBundle) GetOAuthAccessToken(auth_code string) (access string, refresh string, expiration time.Time, err error) {
	// start instrumentation
	config := &oauth.Config{
		ClientId:     r.Config.OAuth.ClientID,
		ClientSecret: r.Config.OAuth.ClientSecret,
		Scope:        "https://www.googleapis.com/auth/userinfo.profile https://www.googleapis.com/auth/userinfo.email",
		AuthURL:      "https://accounts.google.com/o/oauth2/auth",
		TokenURL:     "https://accounts.google.com/o/oauth2/token",
		RedirectURL:  r.Config.OAuth.CallbackURL,
	}
	t := &oauth.Transport{Config: config}
	token, err := t.Exchange(auth_code)
	if err != nil {
		return "", "", time.Time{}, err
	}
	// stop instrumentation
	return token.AccessToken, token.RefreshToken, token.Expiry, nil
}

func (r *RequestBundle) GetAccount(access, refresh string, expiration time.Time) (Account, error) {
	// start instrumentation
	googAccount, err := r.getGoogleAccount(access, refresh, expiration)
	if err != nil {
		r.Log.Error(err.Error())
		return Account{}, err
	}
	account, err := r.getAccountByForeignID(googAccount.ID)
	// add repo call to instrumentation
	if err != nil {
		r.Log.Error(err.Error())
		return Account{}, err
	}
	if !account.IsEmpty() {
		return account, nil
	}
	account = Account{
		Added:         time.Now(),
		Provider:      "google",
		ForeignID:     googAccount.ID,
		Email:         googAccount.Email,
		EmailVerified: googAccount.VerifiedEmail,
		DisplayName:   googAccount.GivenName + " " + googAccount.FamilyName + " (" + googAccount.Email + ")",
		GivenName:     googAccount.GivenName,
		FamilyName:    googAccount.FamilyName,
		Picture:       googAccount.Picture,
		Timezone:      googAccount.Timezone,
		Locale:        googAccount.Locale,
		Gender:        googAccount.Gender,
		UserID:        ruid.RUID(0),
		accessToken:   access,
		refreshToken:  refresh,
		expires:       expiration,
	}
	id, err := gen.Generate([]byte(googAccount.ID))
	if err != nil {
		r.Log.Error(err.Error())
		return Account{}, err
	}
	account.ID = id
	err = r.storeAccount(account, false)
	// add the repo request to the instrumentation
	if err != nil {
		r.Log.Error(err.Error())
		return Account{}, err
	}
	// stop the instrumentation
	return account, nil
}

func (r *RequestBundle) getGoogleAccount(access, refresh string, expiration time.Time) (googleAccount, error) {
	// start instrumentation
	config := &oauth.Config{
		ClientId:     r.Config.OAuth.ClientID,
		ClientSecret: r.Config.OAuth.ClientSecret,
		Scope:        "https://www.googleapis.com/auth/userinfo.profile https://www.googleapis.com/auth/userinfo.email",
		AuthURL:      "https://accounts.google.com/o/oauth2/auth",
		TokenURL:     "https://accounts.google.com/o/oauth2/token",
		RedirectURL:  r.Config.OAuth.CallbackURL,
	}
	token := &oauth.Token{
		AccessToken: access,
	}
	if refresh != "" {
		token.RefreshToken = refresh
	}
	if !expiration.IsZero() {
		token.Expiry = expiration
	}
	t := &oauth.Transport{
		Config: config,
		Token:  token,
	}
	req, err := http.NewRequest("GET", "https://www.googleapis.com/oauth2/v1/userinfo", nil)
	if err != nil {
		r.Log.Error(err.Error())
		return googleAccount{}, err
	}
	resp, err := t.RoundTrip(req)
	if err != nil {
		r.Log.Error(err.Error())
		return googleAccount{}, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		r.Log.Error(err.Error())
		return googleAccount{}, err
	}
	var googAccount googleAccount
	err = json.Unmarshal(body, &googAccount)
	if err != nil {
		r.Log.Error(err.Error())
		return googleAccount{}, err
	}
	if googAccount.Error != nil {
		if googAccount.Error.StatusCode == 401 {
			return googleAccount{}, OAuthAuthError
		} else if googAccount.Error.StatusCode >= 400 {
			return googleAccount{}, OAuthError(googAccount.Error.Message)
		}
	}
	// stop instrumentation
	return googAccount, nil
}

func (r *RequestBundle) getAccountByForeignID(foreign_id string) (Account, error) {
	// start instrumentation
	reply := r.Repo.client.Hget("oauth_foreign_ids_to_accounts", foreign_id)
	// report the request to the repo to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return Account{}, reply.Err
	}
	if reply.Type == redis.ReplyNil {
		r.Log.Warn("Account not found. Foreign ID: %s", foreign_id)
		return Account{}, nil
	}
	account_id, err := reply.Str()
	if err != nil {
		r.Log.Error(err.Error())
		return Account{}, nil
	}
	id, err := ruid.RUIDFromString(account_id)
	if err != nil {
		r.Log.Error(err.Error())
		return Account{}, err
	}
	account, err := r.GetAccountByID(id)
	// report the request to the repo for instrumentation
	if err != nil {
		r.Log.Error(err.Error())
		return Account{}, err
	}
	// stop instrumentation
	return account, nil
}

func (r *RequestBundle) GetAccountByID(id ruid.RUID) (Account, error) {
	// start instrumentation
	reply := r.Repo.client.Hgetall("accounts:" + id.String())
	// report the request to the repo to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return Account{}, reply.Err
	}
	if reply.Type == redis.ReplyNil {
		r.Log.Warn("Account not found. ID: %s", id)
		return Account{}, nil
	}
	hash, err := reply.Hash()
	if err != nil {
		r.Log.Error(err.Error())
		return Account{}, err
	}
	added, err := time.Parse(time.RFC3339, hash["added"])
	if err != nil {
		r.Log.Error(err.Error())
		return Account{}, err
	}
	expires, err := time.Parse(time.RFC3339, hash["expires"])
	if err != nil {
		r.Log.Error(err.Error())
		return Account{}, err
	}
	user_id, err := ruid.RUIDFromString(hash["user_id"])
	if err != nil {
		return Account{}, err
	}
	account := Account{
		Added:         added,
		ID:            id,
		Provider:      hash["provider"],
		ForeignID:     hash["foreign_id"],
		Email:         hash["email"],
		EmailVerified: hash["email_verified"] == "1",
		DisplayName:   hash["display_name"],
		GivenName:     hash["given_name"],
		FamilyName:    hash["family_name"],
		Picture:       hash["picture"],
		Locale:        hash["locale"],
		Timezone:      hash["timezone"],
		Gender:        hash["gender"],
		UserID:        user_id,
		accessToken:   hash["access_token"],
		refreshToken:  hash["refresh_token"],
		expires:       expires,
	}
	// stop instrumentation
	return account, nil
}

func (r *RequestBundle) storeAccount(account Account, update bool) error {
	// start instrumentation
	if update {
		changes := map[string]interface{}{}
		from := map[string]interface{}{}
		old_account, err := r.GetAccountByID(account.ID)
		// report the repo request to instrumentation
		if err != nil {
			r.Log.Error(err.Error())
			return err
		}
		if old_account.Email != account.Email {
			changes["email"] = account.Email
			from["email"] = old_account.Email
		}
		if old_account.EmailVerified != account.EmailVerified {
			changes["email_verified"] = account.EmailVerified
			from["email_verified"] = old_account.EmailVerified
		}
		if old_account.DisplayName != account.DisplayName {
			changes["display_name"] = account.DisplayName
			from["display_name"] = old_account.DisplayName
		}
		if old_account.GivenName != account.GivenName {
			changes["given_name"] = account.GivenName
			from["given_name"] = old_account.GivenName
		}
		if old_account.FamilyName != account.FamilyName {
			changes["family_name"] = account.FamilyName
			from["family_name"] = old_account.FamilyName
		}
		if old_account.Picture != account.Picture {
			changes["picture"] = account.Picture
			from["picture"] = old_account.Picture
		}
		if old_account.Locale != account.Locale {
			changes["locale"] = account.Locale
			from["locale"] = old_account.Locale
		}
		if old_account.Timezone != account.Timezone {
			changes["timezone"] = account.Timezone
			from["timezone"] = old_account.Timezone
		}
		if old_account.Gender != account.Gender {
			changes["gender"] = account.Gender
			from["gender"] = old_account.Gender
		}
		if old_account.UserID != account.UserID {
			changes["user_id"] = account.UserID.String()
			from["user_id"] = old_account.UserID.String()
		}
		if old_account.accessToken != account.accessToken {
			changes["access_token"] = account.accessToken
			from["access_token"] = old_account.accessToken
		}
		if old_account.refreshToken != account.refreshToken {
			changes["refresh_token"] = account.refreshToken
			from["refresh_token"] = old_account.refreshToken
		}
		if !old_account.expires.Equal(account.expires) {
			changes["expires"] = account.expires.Format(time.RFC3339)
			from["expires"] = old_account.expires.Format(time.RFC3339)
		}
		reply := r.Repo.client.Hmset("accounts:"+account.ID.String(), changes)
		// add repo call to instrumentation
		if reply.Err != nil {
			r.Log.Error(reply.Err.Error())
			return reply.Err
		}

		reply = r.Repo.client.Sadd("users:"+account.UserID.String()+":accounts", account.ID.String())
		// add repo call to instrumentation
		if reply.Err != nil {
			r.Log.Error(reply.Err.Error())
			return reply.Err
		}
		r.AuditMap("accounts:"+account.ID.String(), from, changes)
		// add repo call to instrumentation
		return nil
	}
	changes := map[string]interface{}{
		"added":          account.Added.Format(time.RFC3339),
		"provider":       account.Provider,
		"foreign_id":     account.ForeignID,
		"email":          account.Email,
		"email_verified": account.EmailVerified,
		"display_name":   account.DisplayName,
		"given_name":     account.GivenName,
		"family_name":    account.FamilyName,
		"picture":        account.Picture,
		"locale":         account.Locale,
		"timezone":       account.Timezone,
		"gender":         account.Gender,
		"user_id":        account.UserID.String(),
		"access_token":   account.accessToken,
		"refresh_token":  account.refreshToken,
		"expires":        account.expires.Format(time.RFC3339),
	}
	from := map[string]interface{}{
		"added":          "",
		"provider":       "",
		"foreign_id":     "",
		"email":          "",
		"email_verified": "",
		"display_name":   "",
		"given_name":     "",
		"family_name":    "",
		"picture":        "",
		"locale":         "",
		"timezone":       "",
		"gender":         "",
		"user_id":        "",
		"access_token":   "",
		"refresh_token":  "",
		"expires":        "",
	}
	reply := r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		mc.Hmset("accounts:"+account.ID.String(), changes)
		mc.Hmset("oauth_foreign_ids_to_accounts", account.ForeignID, account.ID.String())
		mc.Sadd("users:"+account.UserID.String()+":accounts", account.ID.String())
	})
	// add repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return reply.Err
	}
	r.AuditMap("accounts:"+account.ID.String(), from, changes)
	r.Audit("oauth_foreign_ids_to_accounts", account.ForeignID, "", account.ID.String())
	// report the repo request to instrumentation
	// stop instrumentation
	return nil
}

func (r *RequestBundle) GetAccountsByUser(user User) ([]Account, error) {
	// start instrumentation
	reply := r.Repo.client.Smembers("users:" + user.ID.String() + ":accounts")
	// report the repo request to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return []Account{}, reply.Err
	}
	if reply.Type == redis.ReplyNil {
		r.Log.Warn("User accounts not found. User ID: %s", user.ID)
		return []Account{}, nil
	}
	ids, err := reply.List()
	if err != nil {
		r.Log.Error(err.Error())
		return []Account{}, err
	}
	reply = r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		for _, id := range ids {
			mc.Hgetall("accounts:" + id)
		}
	})
	// report the repo call to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return []Account{}, reply.Err
	}
	accounts := []Account{}
	for pos, elem := range reply.Elems {
		if elem.Type == redis.ReplyNil {
			r.Log.Warn("Account not found: %s", ids[pos])
			continue
		}
		hash, err := elem.Hash()
		if err != nil {
			r.Log.Error(err.Error())
			continue
		}
		added, err := time.Parse(time.RFC3339, hash["added"])
		if err != nil {
			r.Log.Error(err.Error())
			continue
		}
		expires, err := time.Parse(time.RFC3339, hash["expires"])
		if err != nil {
			r.Log.Error(err.Error())
			continue
		}
		user_id, err := ruid.RUIDFromString(hash["user_id"])
		if err != nil {
			r.Log.Error(err.Error())
			continue
		}
		id, err := ruid.RUIDFromString(ids[pos])
		if err != nil {
			r.Log.Error(err.Error())
			continue
		}
		account := Account{
			Added:         added,
			ID:            id,
			Provider:      hash["provider"],
			ForeignID:     hash["foreign_id"],
			Email:         hash["email"],
			EmailVerified: hash["email_verified"] == "1",
			DisplayName:   hash["display_name"],
			GivenName:     hash["given_name"],
			FamilyName:    hash["family_name"],
			Picture:       hash["picture"],
			Locale:        hash["locale"],
			Timezone:      hash["timezone"],
			Gender:        hash["gender"],
			UserID:        user_id,
			accessToken:   hash["access_token"],
			refreshToken:  hash["refresh_token"],
			expires:       expires,
		}
		accounts = append(accounts, account)
	}
	// stop instrumentation
	return accounts, nil
}

func (r *RequestBundle) UpdateAccountTokens(account Account, access, refresh string, expires time.Time) error {
	// start instrumentation
	account.accessToken = access
	account.refreshToken = refresh
	account.expires = expires
	err := r.storeAccount(account, true)
	// report the repo request to instrumentation
	if err != nil {
		return err
	}
	// stop instrumentation
	return nil
}

func (r *RequestBundle) UpdateAccountData(account Account) (Account, error) {
	// start instrumentation
	googAccount, err := r.getGoogleAccount(account.accessToken, account.refreshToken, account.expires)
	if err != nil {
		r.Log.Error(err.Error())
		return Account{}, err
	}
	account.Email = googAccount.Email
	account.EmailVerified = googAccount.VerifiedEmail
	account.DisplayName = googAccount.GivenName + " " + googAccount.FamilyName + " (" + googAccount.Email + ")"
	account.GivenName = googAccount.GivenName
	account.FamilyName = googAccount.FamilyName
	account.Picture = googAccount.Picture
	account.Timezone = googAccount.Timezone
	account.Locale = googAccount.Locale
	account.Gender = googAccount.Gender
	err = r.storeAccount(account, true)
	// add the repo request to the instrumentation
	if err != nil {
		r.Log.Error(err.Error())
		return Account{}, err
	}
	// stop the instrumentation
	return account, nil
}

func (r *RequestBundle) AssociateUserWithAccount(account Account, user ruid.RUID) error {
	// begin instrumentation
	account.UserID = user
	err := r.storeAccount(account, true)
	// report repo request to instrumentation
	if err != nil {
		r.Log.Error(err.Error())
		return err
	}
	// stop instrumentation
	return nil
}

func (r *RequestBundle) DeleteAccount(account Account) error {
	// start instrumentation
	// report the repo request to instrumentation
	// log the changes to the audit log
	// report the repo request to instrumentation
	// stop instrumentation
	return nil
}

func (r *RequestBundle) DeleteAccounts(user User) error {
	// start instrumentation
	// report the repo request to instrumentation
	// log the changes to the audit log
	// report the repo request to instrumentation
	// stop instrumentation
	return nil
}
