package twocloud

import (
	"github.com/fzzbt/radix/redis"
	"secondbit.org/ruid"
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
	ID        ruid.RUID   `json:"id"`
	Key       string      `json:"key"`
	Field     string      `json:"field"`
	From      interface{} `json:"from"`
	To        interface{} `json:"to"`
	IP        string      `json:"ip"`
	User      User        `json:"user"`
	Timestamp time.Time   `json:"timestamp"`
}

func (a *Auditor) Insert(key, ip string, user User, from, to map[string]interface{}) error {
	changes := []Change{}
	for k, v := range to {
		id, err := gen.Generate([]byte(key))
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
			if change.User.ID != ruid.RUID(0) {
				user_str = change.User.ID.String()
			}
			mc.Hmset("audit:"+change.Key+":item:"+change.ID.String(), "from", change.From, "to", change.To, "field", change.Field, "timestamp", change.Timestamp.Format(time.RFC3339), "user", user_str)
			mc.Lpush("audit:"+key, change.ID.String())
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
		err := r.Auditor.Insert(key, r.Request.RemoteAddr, r.AuthUser, from, to)
		if err != nil {
			r.Log.Error(err.Error())
		}
	}
}

func (r *RequestBundle) AuditMap(key string, from, to map[string]interface{}) {
	if r.Auditor != nil {
		err := r.Auditor.Insert(key, r.Request.RemoteAddr, r.AuthUser, from, to)
		if err != nil {
			r.Log.Error(err.Error())
		}
	}
}
