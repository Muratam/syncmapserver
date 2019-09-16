package main

import (
	"fmt"
	"strconv"
	// "time"
)

type User struct {
	AccountName   string // simple
	Address       string
	CreatedAt     int64 // unixtime : time.Time
	ID            int64 // simple
	NumSellItems  int   // simple
	PlainPassword string
	// HashedPassword []byte // eliminated
	// LastBump       time.Time // ??
}

// とりあえず plain password だけを管理するサーバー(ID/AccountName/PlainPassword以外の情報は嘘)
var smUserServer = NewMasterOrSlaveSyncMapServer("127.0.0.1:8888", true, DefaultSendCustomFunction)
var accountNameToIDServer = NewMasterOrSlaveSyncMapServer("127.0.0.1:8889", true, DefaultSendCustomFunction)

func InitUsersSM() {
	if !smUserServer.IsOnThisApp() {
		return
	}
	for _, u := range users {
		id := strconv.Itoa(int(u.ID))
		name := u.AccountName
		smUserServer.Store(id, u)
		accountNameToIDServer.Store(name, id)
	}
}
func RegisterUserSM(u User) {
	id := strconv.Itoa(int(u.ID))
	smUserServer.Store(id, u)
	accountNameToIDServer.Store(u.AccountName, id)
}
func GetPlainPasswordByAccountName(name string) string {
	var u User
	id := ""
	accountNameToIDServer.Load(name, &id)
	smUserServer.Load(id, &u)
	return u.PlainPassword
}
func main() {
	InitUsersSM()
	fmt.Println(GetPlainPasswordByAccountName("nishimura_tetsuhiro"))
	fmt.Println(smUserServer.GetLen())
	fmt.Println(accountNameToIDServer.GetLen())
}
