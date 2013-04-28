package twocloud

import (
	"errors"
	"strings"
	"time"
)

type FundingSource struct {
	ID       ID        `json:"id,omitempty"`
	RemoteID string    `json:"remote_id"`
	Nickname *string   `json:"nickname,omitempty"`
	LastUsed time.Time `json:"last_used,omitempty"`
	Added    time.Time `json:"added,omitempty"`
	UserID   ID        `json:"id,omitempty"`
}

func (fs FundingSource) NicknameSet() bool {
	return fs.Nickname != nil
}

func IsValidProvider(provider string) bool {
	provider = strings.ToLower(provider)
	return provider == "stripe"
}

func (fs FundingSource) GetNickname() string {
	result := ""
	if fs.NicknameSet() {
		result = *fs.Nickname
	}
	return result
}

type FundingSources struct {
	Stripe []Stripe `json:"stripe"`
}

func (p *Persister) GetFundingSourcesByUser(user User) (FundingSources, error) {
	fs := FundingSources{}
	stripeAccounts, err := p.GetStripeSourcesByUser(user)
	if err != nil {
		return FundingSources{}, err
	}
	fs.Stripe = stripeAccounts
	return fs, nil
}

type Chargeable interface {
	Charge(amount int) error
}

var FundingSourceNotFoundError = errors.New("Funding source not found.")
