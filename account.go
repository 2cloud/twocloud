package twocloud

import (
	"database/sql"
	"errors"
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
	row := p.Database.QueryRow("SELECT * FROM accounts WHERE foreign_id=$1", foreign_id)
	err := account.fromRow(row)
	if err == sql.ErrNoRows {
		err = AccountNotFoundError
	}
	return account, err
}

func (p *Persister) createAccount(account Account) error {
	stmt := `INSERT INTO accounts VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);`
	_, err := p.Database.Exec(stmt, account.ID.String(), account.Provider, account.ForeignID, account.Added, account.Email, account.EmailVerified, account.DisplayName, account.GivenName, account.FamilyName, account.Picture, account.Locale, account.Timezone, account.Gender, account.accessToken, account.refreshToken, account.expires, account.UserID.String())
	return err
}

func (p *Persister) GetAccountByID(id ID) (Account, error) {
	var account Account
	row := p.Database.QueryRow("SELECT * FROM accounts WHERE id=$1", id.String())
	err := account.fromRow(row)
	return account, err
}

func (p *Persister) GetAccountsByUser(id ID) ([]Account, error) {
	accounts := []Account{}
	rows, err := p.Database.Query("SELECT * FROM accounts WHERE user_id=$1", id.String())
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
	if access != nil && refresh != nil && !expires.IsZero() {
		stmt := `UPDATE accounts SET access_token=$1, refresh_token=$2, token_expires=$3 WHERE id=$4;`
		_, err = p.Database.Exec(stmt, access, refresh, expires, account.ID.String())
	} else if access != nil && refresh != nil {
		stmt := `UPDATE accounts SET access_token=$1, refresh_token=$2 WHERE id=$3;`
		_, err = p.Database.Exec(stmt, access, refresh, account.ID.String())
	} else if access != nil && !expires.IsZero() {
		stmt := `UPDATE accounts SET access_token=$1, token_expires=$2 WHERE id=$3;`
		_, err = p.Database.Exec(stmt, access, expires, account.ID.String())
	} else if refresh != nil && !expires.IsZero() {
		stmt := `UPDATE accounts SET refresh_token=$1, token_expires=$2 WHERE id=$3;`
		_, err = p.Database.Exec(stmt, refresh, expires, account.ID.String())
	} else if access != nil {
		stmt := `UPDATE accounts SET access_token=$1 WHERE id=$2;`
		_, err = p.Database.Exec(stmt, access, account.ID.String())
	} else if refresh != nil {
		stmt := `UPDATE accounts SET refresh_token=$2 WHERE id=$2;`
		_, err = p.Database.Exec(stmt, refresh, account.ID.String())
	} else if !expires.IsZero() {
		stmt := `UPDATE accounts SET token_expires=$3 WHERE id=$2;`
		_, err = p.Database.Exec(stmt, expires, account.ID.String())
	}
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

	stmt := `UPDATE accounts SET email=$1, email_verified=$2, display_name=$3, given_name=$4, family_name=$5, picture=$6, locale=$7, gender=$8 WHERE id=$9;`
	_, err = p.Database.Exec(stmt, account.Email, account.EmailVerified, account.DisplayName, account.GivenName, account.FamilyName, account.Picture, account.Locale, account.Gender, account.ID.String())
	if err != nil {
		return Account{}, err
	}
	return account, nil
}

func (p *Persister) AssociateUserWithAccount(account Account, user ID) error {
	stmt := `UPDATE accounts SET user_id=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, user.String(), account.ID.String())
	return err
}

func (p *Persister) DeleteAccount(account Account) error {
	stmt := `DELETE FROM accounts WHERE id=$1;`
	_, err := p.Database.Exec(stmt, account.ID.String())
	return err
}

func (p *Persister) DeleteAccounts(user User) error {
	stmt := `DELETE FROM accounts WHERE user_id=$1;`
	_, err := p.Database.Exec(stmt, user.ID.String())
	return err
}
