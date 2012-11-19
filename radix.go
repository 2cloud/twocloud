package twocloud

import (
	"github.com/fzzbt/radix/redis"
)

type Radix struct {
	client *redis.Client
}

func NewRadix(conf redis.Config) *Radix {
	return &Radix{
		client: redis.NewClient(conf),
	}
}

func (r *Radix) Close() {
	r.client.Close()
}
