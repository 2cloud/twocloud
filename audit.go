package twocloud

import (
	"database/sql"
	"github.com/bmizerany/pq"
	"strings"
	"time"
)

type Auditor struct {
	client *sql.DB
}

func NewAuditor(options string) (*Auditor, error) {
	var err error
	if strings.HasPrefix(options, "postgres://") {
		options, err = pq.ParseURL(options)
		if err != nil {
			return nil, err
		}
	}
	conn, err := sql.Open("postgres", options)
	if err != nil {
		return nil, err
	}
	return &Auditor{
		client: conn,
	}, nil
}

func (a *Auditor) Close() {
	a.client.Close()
}

type Change struct {
	ID        uint64      `json:"id"`
	Table     string      `json:"table"`
	Row       uint64      `json:"row"`
	Column    string      `json:"column"`
	From      interface{} `json:"from"`
	To        interface{} `json:"to"`
	Culprit   Culprit     `json:"culprit"`
	Timestamp time.Time   `json:"timestamp"`
}

func (a *Auditor) Insert(persister Persister, table string, row uint64, from, to map[string]interface{}, culprit Culprit) error {
	changes := []Change{}
	for k, v := range to {
		id, err := persister.GetID()
		if err != nil {
			return err
		}
		change := Change{
			ID:        id,
			Table:     table,
			From:      from[k],
			To:        v,
			Column:    k,
			Row:       row,
			Timestamp: time.Now(),
			Culprit:   culprit,
		}
		changes = append(changes, change)
	}
	// TODO: Actually persist the change
	return nil
}

func (persister Persister) Audit(table string, row uint64, column, fromstr, tostr string, culprit Culprit) {
	from := map[string]interface{}{}
	from[column] = fromstr
	to := map[string]interface{}{}
	to[column] = tostr
	err := persister.Auditor.Insert(persister, table, row, from, to, culprit)
	if err != nil {
		persister.Log.Error(err.Error())
	}
}

type Culprit interface {
	GetCulprit() string
}

type Ghost struct{}

func (_ *Ghost) GetCulprit() string {
	return "GHOST"
}
