package twocloud

import (
	"math/rand"
	"time"
)

func GenerateTempCredentials() string {
	cred := ""
	acceptableChars := [50]string{"a", "b", "c", "d", "e", "f", "g", "h", "j", "k", "m", "n", "p", "q", "r", "s", "t", "w", "x", "y", "z", "A", "B", "C", "D", "E", "F", "G", "H", "J", "K", "M", "N", "P", "Q", "R", "S", "T", "W", "X", "Y", "Z", "2", "3", "4", "5", "6", "7", "8", "9"}
	for i := 0; i < 5; i++ {
		rand.Seed(time.Now().UnixNano())
		cred = cred + acceptableChars[rand.Intn(50)]
	}
	return cred
}

// TODO: Need to store temp credentials with an expiration date
func (r *RequestBundle) CreateTempCredentials(user User) ([2]string, error) {
	tmpcred1 := GenerateTempCredentials()
	tmpcred2 := GenerateTempCredentials()
	cred1 := tmpcred1
	cred2 := tmpcred2
	if tmpcred1 > tmpcred2 {
		cred1 = tmpcred2
		cred2 = tmpcred1
	}
	return [2]string{cred1, cred2}, nil
}

// TODO: Need to query the temp credentials and check their expiration date
func (r *RequestBundle) CheckTempCredentials(cred1, cred2 string) (uint64, error) {
	firstcred := cred1
	secondcred := cred2
	if firstcred > secondcred {
		firstcred = cred2
		secondcred = cred1
	}
	// TODO: should return the ID of the user the credentials belong to
	return 0, nil
}
