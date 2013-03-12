package twocloud

import (
	"code.google.com/p/goauth2/oauth"
	"errors"
	"time"
)

type OAuthError string

func (err OAuthError) Error() string {
	return string(err)
}

var OAuthAuthError = errors.New("Invalid OAuth credentials.")

func GetGoogleAuthURL(conf OAuthClient, state string) string {
	config := &oauth.Config{
		ClientId:     conf.ClientID,
		ClientSecret: conf.ClientSecret,
		Scope:        "https://www.googleapis.com/auth/userinfo.profile https://www.googleapis.com/auth/userinfo.email",
		AuthURL:      "https://accounts.google.com/o/oauth2/auth",
		TokenURL:     "https://accounts.google.com/o/oauth2/token",
		RedirectURL:  conf.CallbackURL,
	}
	return config.AuthCodeURL(state)
}

func GetGoogleAccessToken(conf OAuthClient, auth_code string) (access, refresh *string, expiration time.Time, err error) {
	config := &oauth.Config{
		ClientId:     conf.ClientID,
		ClientSecret: conf.ClientSecret,
		Scope:        "https://www.googleapis.com/auth/userinfo.profile https://www.googleapis.com/auth/userinfo.email",
		AuthURL:      "https://accounts.google.com/o/oauth2/auth",
		TokenURL:     "https://accounts.google.com/o/oauth2/token",
		RedirectURL:  conf.CallbackURL,
	}
	t := &oauth.Transport{Config: config}
	token, err := t.Exchange(auth_code)
	if err != nil {
		return nil, nil, time.Time{}, err
	}
	access = &token.AccessToken
	if token.AccessToken == "" {
		access = nil
	}
	refresh = &token.RefreshToken
	if token.RefreshToken == "" {
		refresh = nil
	}
	return access, refresh, token.Expiry, nil
}
