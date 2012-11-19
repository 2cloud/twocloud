package twocloud

import (
	"github.com/fzzbt/radix/redis"
)

type Config struct {
	UseSubscriptions bool         `json:"subscriptions"`
	MaintenanceMode  bool         `json:"maintenance"`
	Database         redis.Config `json:"db"`
	AuditDatabase    redis.Config `json:"audit_db"`
	StatsDatabase    redis.Config `json:"stats_db"`
	OAuth            OAuthClient  `json:"oauth"`
}

type OAuthClient struct {
	ClientID     string
	ClientSecret string
	CallbackURL  string
}
