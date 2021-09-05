package main

import (
	"fmt"
	"github.com/Muratam/syncmapserver"
	"math/rand"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// NewSyncMapServer(GetMasterServerAddress()+":8884", MyServerIsOnMasterServerIP()) のように ISUCON本本では使う
var smMaster = syncmapserver.NewSyncMapServerConn("127.0.0.1:8080", true)
var smSlave = syncmapserver.NewSyncMapServerConn("127.0.0.1:8080", false)
var redisWrap = syncmapserver.NewRedisWrapper("127.0.0.1", 0)
var stores = []syncmapserver.KeyValueStoreConn{smMaster, smSlave, redisWrap}
var names = []string{"smMaster", "smSlave ", "redis   "}

// time.Time は truncateすること。あとpointer型もやめてね
// 大文字のものしか保存されないよ
// 再帰型のスライスも行けるよ
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

func randUser() User {
	return User{
		ID:           int64(random()),
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
func random() int {
	return rand.Intn(100)
}
func randStr() string {
	result := "あいうえおかきくけこ"
	for i := 0; i < 100; i++ {
		result += strconv.Itoa(random())
	}
	return result
}
func ExecuteImpl(times int, isParallel bool, maxGoroutineNum int, f func(i int)) {
	if isParallel {
		var wg sync.WaitGroup
		// GoRoutine の生成コストはかなり高いので、現実的な状況に合わせる
		// 10000件同時接続なんてことはありえないはずなので
		for i := 0; i < maxGoroutineNum; i++ {
			j := i
			wg.Add(1)
			go func() {
				for k := 0; k < times/maxGoroutineNum; k++ {
					f(k*maxGoroutineNum + j)
				}
				wg.Done()
			}()
		}
		wg.Wait()
	} else {
		for i := 0; i < times; i++ {
			f(i)
		}
	}
}
func Execute(times int, isParallel bool, f func(i int)) {
	ExecuteImpl(times, isParallel, syncmapserver.MaxSyncMapServerConnectionNum, f)
}
func Test3(f func(conn syncmapserver.KeyValueStoreConn), times int) (milliSecs []int64) {
	rand.Seed(time.Now().UnixNano())
	fmt.Println("------- ", runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), " x ", times, " -------")
	for i, conn := range stores {
		conn.FlushAll()
		start := time.Now()
		for j := 0; j < times; j++ {
			f(conn)
		}
		duration := time.Now().Sub(start)
		milliSecs = append(milliSecs, int64(duration/time.Millisecond))
		fmt.Println(names[i], ":", milliSecs[i], "ms")
	}
	return milliSecs
}
func TestAverage3(f func(conn syncmapserver.KeyValueStoreConn), times int) {
	milliSecs := make([]int64, len(stores))
	for n := 1; n <= times; n++ {
		resMilliSecs := Test3(f, 1)
		fmt.Println("AVERAGE:")
		for i := 0; i < len(milliSecs); i++ {
			milliSecs[i] += resMilliSecs[i]
			fmt.Println("  ", names[i], ":", milliSecs[i]/int64(n), "ms")
		}
	}
}
