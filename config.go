package twocloud

type Config struct {
	MaintenanceMode bool   `json:"maintenance"`
	Database        string `json:"db"`
	Log             struct {
		Level string `json:"level"`
		File  string `json:"file"`
	} `json:"log"`
	OAuth     OAuthClient `json:"oauth"`
	Generator IDGenerator `json:"id_gen"`
	Stripe    string      `json:"stripe"`
	NSQ       NSQ         `json:"nsq"`
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

type NSQ struct {
	Address string `json:"address"`
}
