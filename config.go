package twocloud

import (
	"time"
)

type Config struct {
	UseSubscriptions bool   `json:"subscriptions"`
	MaintenanceMode  bool   `json:"maintenance"`
	Database         string `json:"db"`
	Auditor          string `json:"audit_db"`
	Log              struct {
		Level string `json:"level"`
		File  string `json:"file"`
	} `json:"log"`
	OAuth       OAuthClient   `json:"oauth"`
	TrialPeriod time.Duration `json:"trial_period"`
	GracePeriod time.Duration `json:"grace_period"`
	Generator   IDGenerator   `json:"id_gen"`
	Culprit     string        `json:"culprit"`
	Stripe      string        `json:"stripe"`
}

type OAuthClient struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	CallbackURL  string `json:"callback"`
}

type IDGenerator struct {
	Address string `json:"address"`
	Token   string `json:"token"`
}
