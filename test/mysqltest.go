package syncmapserver

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// mysql vs syncmap[Master/Slave/Slave]
type User struct {
	ID           int64     `json:"id" db:"id"`
	AccountName  string    `json:"account_name" db:"account_name"`
	Address      string    `json:"address,omitempty" db:"address"`
	NumSellItems int       `json:"num_sell_items" db:"num_sell_items"`
	LastBump     time.Time `json:"-" db:"last_bump"`
	CreatedAt    time.Time `json:"-" db:"created_at"`
}

var localUserMap4000 map[string]interface{}
var keys4000 []string

func randUser(id int) User {
	return User{
		ID:           int64(id),
		AccountName:  randStr(),
		Address:      randStr(),
		NumSellItems: random(),
		LastBump:     time.Now().Truncate(time.Second),
		CreatedAt:    time.Now().Truncate(time.Second),
	}
}
func InitForBenchMGetMSetUser4000() {
	localUserMap4000 = map[string]interface{}{}
	for i := 0; i < 4000; i++ {
		key := randStr()
		localUserMap4000[key] = randUser()
		keys4000 = append(keys4000, key)
	}
}

func assert(cond bool) {
	if !cond {
		panic("assertion failed")
	}
}

var smMaster = NewSyncMapServerConn("127.0.0.1:8080", true)
var smSlave1 = NewSyncMapServerConn("127.0.0.1:8080", false)
var smSlave2 = NewSyncMapServerConn("127.0.0.1:8080", false)
var sms = []*SyncMapServerConn{smMaster, smSlave1, smSlave2}
var initNum = 4000
var insertNum = 4000
var updateNum = 4000
var selectNum = 8000

func BenchSMS() {
	rand.Seed(time.Now().UnixNano())
	fmt.Println("------- Slave+Master+Slave -------")
	smMaster.FlushAll()
	for i := 1; i <= initNum; i++ {
		u := randUser(i)
		smMaster.Set(strconv.Itoa(i), u)
	}
	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < insertNum; i++ {
		j := i
		sm := sms[j%3]
		sm.Insert()
	}
	wg.Wait()
	duration := time.Now().Sub(start)
	milliSec := int64(duration / time.Millisecond)
	fmt.Println("SMS:", milliSec, "ms")

}

func main() {

}
