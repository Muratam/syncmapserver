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
var smUserSlaveServer = NewMasterOrSlaveSyncMapServer("127.0.0.1:8888", false, DefaultSendCustomFunction)
var accountNameToIDSlaveServer = NewMasterOrSlaveSyncMapServer("127.0.0.1:8889", false, DefaultSendCustomFunction)

func InitUsersSM() {
	if !smUserServer.IsOnThisApp() {
		return
	}
	smUserServer.ClearAll()
	accountNameToIDServer.ClearAll()
	for _, u := range users {
		id := strconv.Itoa(int(u.ID))
		name := u.AccountName
		smUserServer.Store(id, u)
		accountNameToIDServer.Store(name, id)
	}
}
func RegisterUserSM(u User) {
	id := strconv.Itoa(int(u.ID))
	smUserSlaveServer.Store(id, u)
	accountNameToIDSlaveServer.Store(u.AccountName, id)
}
func GetPlainPasswordByAccountName(name string) string {
	var u User
	id := ""
	accountNameToIDSlaveServer.Load(name, &id)
	smUserSlaveServer.Load(id, &u)
	return u.PlainPassword
}
func transactionTest() {
	for i := 0; i < 1000; i++ {
		go func() {
			if smUserSlaveServer.IsLockedKey("100") {
				return
			}
			smUserSlaveServer.StartTransactionWithKey("100", func(tx *SyncMapServerTransaction) {
				var u User
				tx.Load("100", &u)
				u.NumSellItems += 1
				tx.Store("100", u)
				fmt.Println(u)
			})
		}()
	}
}
func main() {
	InitUsersSM()
	fmt.Println(GetPlainPasswordByAccountName("nishimura_tetsuhiro"))
	fmt.Println(smUserSlaveServer.GetLen())
	fmt.Println(accountNameToIDSlaveServer.GetLen())
	keys := make([]string, 0)
	for i := 100; i < 200; i++ {
		uid := strconv.Itoa(i)
		var u User
		smUserSlaveServer.Load(uid, &u)
		fmt.Println(u)
		keys = append(keys, uid)
	}
	fmt.Println(keys)
	values := smUserSlaveServer.MultiLoad(keys)
	for i, _ := range keys {
		var u User
		value := values[i]
		if len(value) == 0 {
			continue
		}
		DecodeFromBytes(value, &u)
		fmt.Println(u)
	}
	// transactionTest()
	// for i := 0; i < 50; i++ {
	// 	time.Sleep(time.Duration(1) * time.Second)
	// 	fmt.Println(i)
	// }
	// for i := 0; i < 10; i++ {
	// 	fmt.Println(GetPlainPasswordByAccountName("nishimura_tetsuhiro"))
	// 	fmt.Println(smUserSlaveServer.GetLen())
	// 	fmt.Println(accountNameToIDSlaveServer.GetLen())
	// }
}
