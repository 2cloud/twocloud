package twocloud

import (
	"net/http"
	"secondbit.org/ruid"
	"time"
)

var gen *ruid.Generator

func init() {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		panic(err.Error())
	}
	epoch := time.Date(2010, time.December, 2, 0, 0, 0, 0, location)
	gen = ruid.NewGenerator(epoch)
}

type RequestBundle struct {
	Repo   *Radix
	Config Config
	Log    *Log
	// Cache
	Auditor *Auditor
	// Instrumentor
	// Instrument
	Request *http.Request
	AuthUser User
}
