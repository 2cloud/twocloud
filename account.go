package twocloud

import (
	"time"
)

var AccountTableCreateStatement = `CREATE TABLE accounts (
	id bigint primary key,
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
	user_id bigint);`

type Account struct {
	Added    time.Time `json:"added,omitempty"`
	ID       uint64    `json:"id,omitempty"`
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
	UserID       uint64 `json:"-"`
	accessToken  string
	refreshToken string
	expires      time.Time
}

func (account *Account) IsEmpty() bool {
	return account.ID == 0
}

func (account *Account) fromRow(row ScannableRow) error {
	return row.Scan(&account.ID, &account.Provider, &account.ForeignID, &account.Added, &account.Email, &account.EmailVerified, &account.DisplayName, &account.GivenName, &account.FamilyName, &account.Picture, &account.Locale, &account.Timezone, &account.Gender, &account.accessToken, &account.refreshToken, &account.expires, account.UserID)
}

func (p *Persister) GetAccountByTokens(access, refresh string, expiration time.Time) (Account, error) {
	googAccount, err := getGoogleAccount(p.Config.OAuth, access, refresh, expiration)
	if err != nil {
		p.Log.Error(err.Error())
		return Account{}, err
	}
	account, err := p.getAccountByForeignID(googAccount.ID)
	if err != nil {
		p.Log.Error(err.Error())
		return Account{}, err
	}
	if !account.IsEmpty() {
		return account, nil
	}
	account = googAccount.toAccount(access, refresh, expiration)
	id, err := p.GetID()
	if err != nil {
		p.Log.Error(err.Error())
		return Account{}, err
	}
	account.ID = id
	err = p.createAccount(account)
	if err != nil {
		p.Log.Error(err.Error())
		return Account{}, err
	}
	return account, nil
}

func (p *Persister) getAccountByForeignID(foreign_id string) (Account, error) {
	var account Account
	row := p.Database.QueryRow("SELECT * FROM accounts WHERE foreign_id=$1", foreign_id)
	err := account.fromRow(row)
	return account, err
}

func (p *Persister) createAccount(account Account) error {
	stmt := `INSERT INTO accounts VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);`
	_, err := p.Database.Exec(stmt, account.ID, account.Provider, account.ForeignID, account.Added, account.Email, account.EmailVerified, account.DisplayName, account.GivenName, account.FamilyName, account.Picture, account.Locale, account.Timezone, account.Gender, account.accessToken, account.refreshToken, account.expires, account.UserID)
	return err
}

func (p *Persister) GetAccountByID(id uint64) (Account, error) {
	var account Account
	row := p.Database.QueryRow("SELECT * FROM accounts WHERE id=$1", id)
	err := account.fromRow(row)
	return account, err
}

func (p *Persister) GetAccountsByUser(id uint64) ([]Account, error) {
	accounts := []Account{}
	rows, err := p.Database.Query("SELECT * FROM accounts WHERE user_id=$1", id)
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

func (p *Persister) UpdateAccountTokens(account Account, access, refresh string, expires time.Time) error {
	stmt := `UPDATE accounts SET access_token=$1, refresh_token=$2, token_expires=$3 WHERE id=$4;`
	_, err := p.Database.Exec(stmt, access, refresh, expires, account.ID)
	return err
}

/*func (r *RequestBundle) UpdateAccountData(account Account) (Account, error) {
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
}*/

func (p *Persister) AssociateUserWithAccount(account Account, user uint64) error {
	stmt := `UPDATE accounts SET user_id=$1 WHERE id=$2;`
	_, err := p.Database.Exec(stmt, user, account.ID)
	return err
}

func (p *Persister) DeleteAccount(account Account) error {
	stmt := `DELETE FROM accounts WHERE id=$1;`
	_, err := p.Database.Exec(stmt, account.ID)
	return err
}

func (p *Persister) DeleteAccounts(user User) error {
	stmt := `DELETE FROM accounts WHERE user_id=$1;`
	_, err := p.Database.Exec(stmt, user.ID)
	return err
}
