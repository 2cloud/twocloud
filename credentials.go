package twocloud

import (
	"math/rand"
	"secondbit.org/pan"
	"time"
)

var CredentialsTableCreateStatement = `CREATE TABLE temp_credentials (
	user_id varchar NOT NULL,
	first varchar(5) NOT NULL,
	second varchar(5) NOT NULL,
        expires timestamp NOT NULL);`

func GenerateTempCredentials() string {
	cred := ""
	acceptableChars := [50]string{"a", "b", "c", "d", "e", "f", "g", "h", "j", "k", "m", "n", "p", "q", "r", "s", "t", "w", "x", "y", "z", "A", "B", "C", "D", "E", "F", "G", "H", "J", "K", "M", "N", "P", "Q", "R", "S", "T", "W", "X", "Y", "Z", "2", "3", "4", "5", "6", "7", "8", "9"}
	for i := 0; i < 5; i++ {
		rand.Seed(time.Now().UnixNano())
		cred = cred + acceptableChars[rand.Intn(50)]
	}
	return cred
}

const CredentialsCreatedTopic = "creds.created"

func (p *Persister) CreateTempCredentials(user User) ([2]string, error) {
	tmpcred1 := GenerateTempCredentials()
	tmpcred2 := GenerateTempCredentials()
	cred1 := tmpcred1
	cred2 := tmpcred2
	if tmpcred1 > tmpcred2 {
		cred1 = tmpcred2
		cred2 = tmpcred1
	}
	query := pan.New()
	query.SQL = "INSERT INTO temp_credentials VALUES ("
	query.Include("?", user.ID.String())
	query.Include("?", cred1)
	query.Include("?", cred2)
	query.Include("?", time.Now().Add(time.Minute*5))
	query.FlushExpressions(", ")
	query.SQL += ")"
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err == nil {
		_, nsqErr := p.Publish(CredentialsCreatedTopic, &user.ID, nil, nil)
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return [2]string{cred1, cred2}, err
}

func (p *Persister) CheckTempCredentials(cred1, cred2 string) (ID, error) {
	firstcred := cred1
	secondcred := cred2
	if firstcred > secondcred {
		firstcred = cred2
		secondcred = cred1
	}
	var userIDStr string
	query := pan.New()
	query.SQL = "SELECT user_id FROM temp_credentials"
	query.IncludeWhere()
	query.Include("first=?", firstcred)
	query.Include("second=?", secondcred)
	query.Include("expires > ?", time.Now())
	row := p.Database.QueryRow(query.Generate(" "), query.Args...)
	err := row.Scan(&userIDStr)
	if err != nil {
		return ID(0), err
	}
	user, err := IDFromString(userIDStr)
	return user, err
}

func (p *Persister) ClearExpiredCredentials() error {
	query := pan.New()
	query.SQL = "DELETE FROM temp_credentials"
	query.IncludeWhere()
	query.Include("expires < ?", time.Now())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	return err
}
