package twocloud

import (
	"github.com/noeq/noeq"
	"net/http"
)

type RequestBundle struct {
	Generator *noeq.Client
	Repo      *Radix
	Config    Config
	Log       *Log
	// Cache
	Auditor *Auditor
	// Instrumentor
	// Instrument
	Request  *http.Request
	AuthUser User
	Device   Device
}

func (rb *RequestBundle) GetID() (id uint64, err error) {
	for trys := 5; trys > 0; trys-- {
		id, err = rb.Generator.GenOne()
		if err != nil {
			rb.Log.Error(err.Error())
			continue
		}
		return
	}
	rb.Log.Error("No ID generated.")
	return
}
