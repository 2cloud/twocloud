package twocloud

import (
	"math/rand"
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

func (p *Persister) CreateTempCredentials(user User) ([2]string, error) {
	tmpcred1 := GenerateTempCredentials()
	tmpcred2 := GenerateTempCredentials()
	cred1 := tmpcred1
	cred2 := tmpcred2
	if tmpcred1 > tmpcred2 {
		cred1 = tmpcred2
		cred2 = tmpcred1
	}
	stmt := `INSERT INTO temp_credentials VALUES ($1, $2, $3, $4);`
	_, err := p.Database.Exec(stmt, user.ID, cred1, cred2, time.Now().Add(time.Minute*5))
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
	row := p.Database.QueryRow("SELECT user_id FROM temp_credentials WHERE first=$1 and second=$2 and expires > $3", firstcred, secondcred, time.Now())
	err := row.Scan(&userIDStr)
	if err != nil {
		return ID(0), err
	}
	user, err := IDFromString(userIDStr)
	return user, err
}

func (p *Persister) ClearExpiredCredentials() error {
	stmt := `DELETE FROM temp_credentials WHERE expires < $1;`
	_, err := p.Database.Exec(stmt, time.Now())
	return err
}
