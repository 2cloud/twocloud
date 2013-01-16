package twocloud

import (
	"time"
)

type StripeAccount struct {
	ID          uint64    `json:"id,omitempty"`
	ForeignID   string    `json:"foreign_id,omitempty"`
	ScheduleID  string    `json:"schedule_id,omitempty"`
	LastCharged time.Time `json:"last_charged,omitempty"`
}

// Create a Stripe customer
// Charge a Stripe customer
// Schedule recurring payments
// Cancel recurring payments
