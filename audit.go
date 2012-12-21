package twocloud

import (
	"github.com/fzzbt/radix/redis"
	"strconv"
	"time"
)

type Auditor struct {
	client *redis.Client
}

func NewAuditor(conf redis.Config) *Auditor {
	return &Auditor{
		client: redis.NewClient(conf),
	}
}

func (a *Auditor) Close() {
	a.client.Close()
}

type Change struct {
	ID        uint64      `json:"id"`
	Key       string      `json:"key"`
	Field     string      `json:"field"`
	From      interface{} `json:"from"`
	To        interface{} `json:"to"`
	IP        string      `json:"ip"`
	User      User        `json:"user"`
	Timestamp time.Time   `json:"timestamp"`
}

func (a *Auditor) Insert(r *RequestBundle, key, ip string, user User, from, to map[string]interface{}) error {
	changes := []Change{}
	for k, v := range to {
		id, err := r.GetID()
		if err != nil {
			return err
		}
		change := Change{
			ID:        id,
			Key:       key,
			From:      from[k],
			To:        v,
			Field:     k,
			Timestamp: time.Now(),
			IP:        ip,
			User:      user,
		}
		changes = append(changes, change)
	}
	reply := a.client.MultiCall(func(mc *redis.MultiCall) {
		for _, change := range changes {
			user_str := ""
			if change.User.ID != 0 {
				user_str = strconv.FormatUint(change.User.ID, 10)
			}
			mc.Hmset("audit:"+change.Key+":item:"+strconv.FormatUint(change.ID, 10), "from", change.From, "to", change.To, "field", change.Field, "timestamp", change.Timestamp.Format(time.RFC3339), "user", user_str)
			mc.Lpush("audit:"+key, change.ID)
		}
	})
	return reply.Err
}

func (r *RequestBundle) Audit(key, field, fromstr, tostr string) {
	if r.Auditor != nil {
		from := map[string]interface{}{}
		from[field] = fromstr
		to := map[string]interface{}{}
		to[field] = tostr
		err := r.Auditor.Insert(r, key, r.Request.RemoteAddr, r.AuthUser, from, to)
		if err != nil {
			r.Log.Error(err.Error())
		}
	}
}

func (r *RequestBundle) AuditMap(key string, from, to map[string]interface{}) {
	if r.Auditor != nil {
		err := r.Auditor.Insert(r, key, r.Request.RemoteAddr, r.AuthUser, from, to)
		if err != nil {
			r.Log.Error(err.Error())
		}
	}
}
