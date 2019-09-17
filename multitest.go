package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// MULTIGET / MULTISET のテスト
// isucon9q
type User struct {
	ID             int64     `json:"id" db:"id"`
	AccountName    string    `json:"account_name" db:"account_name"`
	HashedPassword []byte    `json:"-" db:"hashed_password"`
	Address        string    `json:"address,omitempty" db:"address"`
	NumSellItems   int       `json:"num_sell_items" db:"num_sell_items"`
	LastBump       time.Time `json:"-" db:"last_bump"`
	CreatedAt      time.Time `json:"-" db:"created_at"`
	PlainPassword  string
}

var smUserServer = NewMasterOrSlaveSyncMapServer("127.0.0.1:8888", os.Getenv("IS_MASTER") != "", DefaultSendCustomFunction)

func setSingle() {
	// SET を使って 4000データ作成
	for i := 1; i <= 4000; i++ {
		is := strconv.Itoa(i)
		u := User{
			ID:            int64(i),
			AccountName:   "NAME-" + is,
			Address:       "ADDR-" + is,
			NumSellItems:  i,
			LastBump:      time.Now(),
			CreatedAt:     time.Now(),
			PlainPassword: "PLAIN-" + is,
		}
		smUserServer.Store(is, u)
	}
}

func getSingle() {
	// GET を使って 4000データロード
	for i := 1; i <= 4000; i++ {
		is := strconv.Itoa(i)
		u := User{}
		smUserServer.Load(is, &u)
		fmt.Print(u.ID, " ")
	}
}
func getMulti() {
	// MGETを使って4000データロード
	keys := make([]string, 0)
	for i := 1; i <= 4000; i++ {
		keys = append(keys, strconv.Itoa(i))
	}
	values := smUserServer.MultiLoad(keys)
	for i, _ := range keys {
		value := values[i]
		u := User{}
		DecodeFromBytes(value, &u)
		fmt.Print(u)
	}
}

func main() {
	// setSingle()
	getMulti()
	for {
		time.Sleep(1000 * time.Millisecond)
	}
}
