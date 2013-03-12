package twocloud

import (
	"code.google.com/p/goauth2/oauth"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"
)

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

func getGoogleAccount(conf OAuthClient, access, refresh *string, expiration time.Time) (googleAccount, error) {
	config := &oauth.Config{
		ClientId:     conf.ClientID,
		ClientSecret: conf.ClientSecret,
		Scope:        "https://www.googleapis.com/auth/userinfo.profile https://www.googleapis.com/auth/userinfo.email",
		AuthURL:      "https://accounts.google.com/o/oauth2/auth",
		TokenURL:     "https://accounts.google.com/o/oauth2/token",
		RedirectURL:  conf.CallbackURL,
	}
	token := &oauth.Token{
		AccessToken: *access,
	}
	if refresh != nil {
		token.RefreshToken = *refresh
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
		return googleAccount{}, err
	}
	resp, err := t.RoundTrip(req)
	if err != nil {
		return googleAccount{}, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return googleAccount{}, err
	}
	var googAccount googleAccount
	err = json.Unmarshal(body, &googAccount)
	if err != nil {
		return googleAccount{}, err
	}
	if googAccount.Error != nil {
		if googAccount.Error.StatusCode == 401 {
			return googleAccount{}, OAuthAuthError
		} else if googAccount.Error.StatusCode >= 400 {
			return googleAccount{}, OAuthError(googAccount.Error.Message)
		}
	}
	return googAccount, nil
}

func (googAccount *googleAccount) toAccount(access, refresh *string, expiration time.Time) Account {
	return Account{
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
		UserID:        0,
		accessToken:   access,
		refreshToken:  refresh,
		expires:       expiration,
	}
}
