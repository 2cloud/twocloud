package twocloud

import (
	"github.com/fzzbt/radix/redis"
	"time"
)

type Config struct {
	UseSubscriptions        bool              `json:"subscriptions"`
	MaintenanceMode         bool              `json:"maintenance"`
	Database                redis.Config      `json:"db"`
	AuditDatabase           redis.Config      `json:"audit_db"`
	InstrumentationDatabase redis.Config      `json:"instrumentation_db"`
	OAuth                   OAuthClient       `json:"oauth"`
	TrialPeriod             time.Duration     `json:"trial_period"`
	GracePeriod             time.Duration     `json:"grace_period"`
	Generator               IDGenerator       `json:"id_gen"`
}

type OAuthClient struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	CallbackURL  string `json:"callback"`
}

type IDGenerator struct {
	Address string `json:"address"`
	Token string `json:"token"`
}
