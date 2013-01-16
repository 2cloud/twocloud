package twocloud

type DwollaAccount struct {
	ID uint64 `json:"id,omitempty"`
	AccessToken string `json:"-"`
	PIN string `json:"-"`
	ScheduleID string `json:"schedule_id,omitempty"`
	LastCharged time.Time `json:"last_charged,omitempty"`
}

// Get a Dwolla OAuth redirect URL
// Receive a Dwolla OAuth callback
// Save a Dwolla Account with PIN
// Charge a Dwolla account
// Encrypt the PIN
// Decrypt the PIN
// Determine if the PIN is encrypted
// Determine if the PIN is set
// Schedule recurring payments
// Cancel recurring payments
