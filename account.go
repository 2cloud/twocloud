package twocloud

import (
	"database/sql"
	"errors"
	"secondbit.org/pan"
	"time"
)

var AccountTableCreateStatement = `CREATE TABLE accounts (
	id varchar primary key,
	provider varchar NOT NULL,
	foreign_id varchar NOT NULL,
	added timestamp default CURRENT_TIMESTAMP,
	email varchar NOT NULL,
	email_verified bool default false,
	display_name varchar,
	given_name varchar,
	family_name varchar,
	picture varchar,
	locale varchar,
	timezone varchar,
	gender varchar,
	access_token varchar,
	refresh_token varchar,
	token_expires timestamp,
	user_id varchar);`

type Account struct {
	Added    time.Time `json:"added,omitempty"`
	ID       ID        `json:"id,omitempty"`
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
	UserID       ID `json:"-"`
	accessToken  *string
	refreshToken *string
	expires      time.Time
}

func (account *Account) IsEmpty() bool {
	return account.ID.IsZero()
}

var AccountNotFoundError = errors.New("Account not found.")

func (account *Account) fromRow(row ScannableRow) error {
	var accountIDStr string
	var userIDStr sql.NullString
	err := row.Scan(&accountIDStr, &account.Provider, &account.ForeignID, &account.Added, &account.Email, &account.EmailVerified, &account.DisplayName, &account.GivenName, &account.FamilyName, &account.Picture, &account.Locale, &account.Timezone, &account.Gender, &account.accessToken, &account.refreshToken, &account.expires, &userIDStr)
	if err != nil {
		return err
	}
	accountID, err := IDFromString(accountIDStr)
	if err != nil {
		return err
	}
	account.ID = accountID
	account.UserID = ID(0)
	if userIDStr.Valid {
		userID, err := IDFromString(userIDStr.String)
		if err != nil {
			return err
		}
		account.UserID = userID
	}
	return nil
}

func (p *Persister) GetAccountByTokens(access, refresh *string, expiration time.Time) (Account, error) {
	googAccount, err := getGoogleAccount(p.Config.OAuth, access, refresh, expiration)
	if err != nil {
		return Account{}, err
	}
	account, err := p.getAccountByForeignID(googAccount.ID)
	if err != nil && err != AccountNotFoundError {
		return Account{}, err
	}
	if !account.IsEmpty() {
		return account, nil
	}
	account = googAccount.toAccount(access, refresh, expiration)
	id, err := p.GetID()
	if err != nil {
		return Account{}, err
	}
	account.ID = id
	err = p.createAccount(account)
	if err != nil {
		return Account{}, err
	}
	return account, nil
}

func (p *Persister) getAccountByForeignID(foreign_id string) (Account, error) {
	var account Account
	query := pan.New()
	query.SQL = "SELECT * FROM accounts"
	query.IncludeWhere()
	query.Include("foreign_id=?", foreign_id)
	row := p.Database.QueryRow(query.Generate(" "), query.Args...)
	err := account.fromRow(row)
	if err == sql.ErrNoRows {
		err = AccountNotFoundError
	}
	return account, err
}

func (p *Persister) createAccount(account Account) error {
	query := pan.New()
	query.SQL = "INSERT INTO accounts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	query.Args = append(query.Args, account.ID.String(), account.Provider, account.ForeignID, account.Added, account.Email, account.EmailVerified, account.DisplayName, account.GivenName, account.FamilyName, account.Picture, account.Locale, account.Timezone, account.Gender, account.accessToken, account.refreshToken, account.expires, account.UserID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}

func (p *Persister) GetAccountByID(id ID) (Account, error) {
	var account Account
	query := pan.New()
	query.SQL = "SELECT * FROM accounts"
	query.IncludeWhere()
	query.Include("id=?", id.String())
	row := p.Database.QueryRow(query.Generate(" "), query.Args...)
	err := account.fromRow(row)
	return account, err
}

func (p *Persister) GetAccountsByUser(id ID) ([]Account, error) {
	accounts := []Account{}
	query := pan.New()
	query.SQL = "SELECT * FROM accounts"
	query.IncludeWhere()
	query.Include("user_id=?", id.String())
	rows, err := p.Database.Query(query.Generate(" "), query.Args...)
	if err != nil {
		return []Account{}, err
	}
	for rows.Next() {
		var account Account
		err = account.fromRow(rows)
		if err != nil {
			return []Account{}, err
		}
		accounts = append(accounts, account)
	}
	err = rows.Err()
	return accounts, err
}

func (p *Persister) UpdateAccountTokens(account Account, access, refresh *string, expires time.Time) error {
	var err error
	query := pan.New()
	query.SQL = "UPDATE accounts SET"
	query.IncludeIfNotNil("access_token=?", access)
	query.IncludeIfNotNil("refresh_token=?", refresh)
	query.IncludeIfNotEmpty("token_expires=?", expires)
	query.FlushExpressions(" and ")
	query.IncludeWhere()
	query.Include("id=?", account.ID.String())
	_, err = p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}

func (p *Persister) UpdateAccountData(account Account) (Account, error) {
	googAccount, err := getGoogleAccount(p.Config.OAuth, account.accessToken, account.refreshToken, account.expires)
	if err != nil {
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

	query := pan.New()
	query.SQL = "UPDATE accounts SET "
	query.Include("email=?", account.Email)
	query.Include("email_verified=?", account.EmailVerified)
	query.Include("display_name=?", account.DisplayName)
	query.Include("given_name=?", account.GivenName)
	query.Include("family_name=?", account.FamilyName)
	query.Include("picture=?", account.Picture)
	query.Include("locale=?", account.Locale)
	query.Include("gender=?", account.Gender)
	query.FlushExpressions(", ")
	query.IncludeWhere()
	query.Include("id=?", account.ID.String())
	_, err = p.Database.Exec(query.Generate(" "), query.Args...)
	if err != nil {
		return Account{}, err
	}
	return account, nil
}

func (p *Persister) AssociateUserWithAccount(account Account, user ID) error {
	query := pan.New()
	query.SQL = "UPDATE accounts SET "
	query.Include("user_id=?", user.String())
	query.IncludeWhere()
	query.Include("id=?", account.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}

func (p *Persister) DeleteAccount(account Account) error {
	query := pan.New()
	query.SQL = "DELETE FROM accounts"
	query.IncludeWhere()
	query.Include("id=?", account.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}

func (p *Persister) DeleteAccounts(user User) error {
	query := pan.New()
	query.SQL = "DELETE FROM accounts"
	query.IncludeWhere()
	query.Include("user_id=?", user.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}
