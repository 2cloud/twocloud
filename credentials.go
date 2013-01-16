package twocloud

import (
	"math/rand"
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

func (r *RequestBundle) CreateTempCredentials(user User) ([2]string, error) {
	// start instrumentation
	tmpcred1 := GenerateTempCredentials()
	tmpcred2 := GenerateTempCredentials()
	cred1 := tmpcred1
	cred2 := tmpcred2
	if tmpcred1 > tmpcred2 {
		cred1 = tmpcred2
		cred2 = tmpcred1
	}
	reply := r.Repo.client.MultiCall(func(mc *redis.MultiCall) {
		mc.Set("tokens:"+cred1+":"+cred2, user.ID)
		mc.Expire("tokens:"+cred1+":"+cred2, "300")
	})
	// add the repo request to instrumentation
	if reply.Err != nil {
		return [2]string{"", ""}, reply.Err
	}
	for _, rep := range reply.Elems {
		if rep.Err != nil {
			return [2]string{"", ""}, rep.Err
		}
	}
	r.Audit("tokens:"+strconv.FormatUint(user.ID, 10), cred1, "", cred2)
	// add the repo requests to instrumentation
	return [2]string{cred1, cred2}, nil
}

func (r *RequestBundle) CheckTempCredentials(cred1, cred2 string) (uint64, error) {
	// start instrumentation
	firstcred := cred1
	secondcred := cred2
	if firstcred > secondcred {
		firstcred = cred2
		secondcred = cred1
	}
	reply := r.Repo.client.Get("tokens:" + firstcred + ":" + secondcred)
	// add the repo request to instrumentation
	if reply.Err != nil {
		r.Log.Error(reply.Err.Error())
		return uint64(0), reply.Err
	}
	if reply.Type == redis.ReplyNil {
		// add invalid credential error to stats
		// add the repo requests to instrumentation
		return uint64(0), InvalidCredentialsError
	}
	val, err := reply.Str()
	if err != nil {
		r.Log.Error(err.Error())
		return uint64(0), err
	}
	id, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		r.Log.Error(err.Error())
		return uint64(0), err
	}
	return id, nil
	// stop instrumentation
}
