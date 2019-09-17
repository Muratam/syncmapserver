package main

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"strconv"
	"time"
)

// Redis と SyncMapSercer の違いを吸収
type KeyValueStoreCore interface { // ptr は参照を着けてLoadすることを示す
	// Normal Command
	Get(key string, value interface{}) bool // ptr (キーが無ければ false)
	Set(key string, value interface{})
	MGet(keys []string) MGetResult     // 改めて Get するときに ptr
	MSet(store map[string]interface{}) // 先に対応Mapを作りそれをMSet
	Exists(key string) bool
	Del(key string)
	IncrBy(key string, value int) int
	DBSize() int // means key count
	// Keys() []string TODO:
	FlushAll()
	// InitList(key string)                     // SyncMapServerの方だけ必要
	// RPush(key string, value interface{}) int // Push後の 自身の index を返す
	// LLen(key string) int
	// LIndex(key string, index int, value interface{}) bool // ptr (キーが無ければ false)
	// LSet(key string, index int, value interface{})
	// LRange(key string, start, stop int, values []interface{}) // ptr(0,-1 で全て取得可能) TODO:
}

type KeyValueStore interface {
	KeyValueStoreCore
	// StartTransaction(f func(tx *KeyValueStoreCore))
}

/////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////

func assert(cond bool) {
	if !cond {
		panic("assertion failed")
	}
}
func random() int {
	return rand.Intn(100)
}
func randStr() string {
	result := ""
	for i := 0; i < 100; i++ {
		result += strconv.Itoa(random())
	}
	return result
}

func TestGetSetInt(store KeyValueStore) {
	// int を Get して Set するだけの 一番基本的なやつ
	n := random()
	x := 0
	store.Set("x", n)
	store.Get("x", &x)
	assert(x == n)
	store.Set("y", n*2)
	store.Get("y", &x)
	assert(x == n*2)
	store.Get("x", &x)
	assert(x == n)
	ok := store.Get("nop", &x)
	assert(!ok)
}
func TestGetSetUser(store KeyValueStore) {
	// userデータ を Get して Set するだけ
	// Pointer 型 は渡せないことに注意。struct in struct は多分大丈夫。
	type User struct {
		ID           int64     `json:"id" db:"id"`
		AccountName  string    `json:"account_name" db:"account_name"`
		Address      string    `json:"address,omitempty" db:"address"`
		NumSellItems int       `json:"num_sell_items" db:"num_sell_items"`
		LastBump     time.Time `json:"-" db:"last_bump"`
		CreatedAt    time.Time `json:"-" db:"created_at"`
	}
	u := User{
		ID:           int64(random()),
		AccountName:  randStr(),
		Address:      randStr(),
		NumSellItems: random(),
		LastBump:     time.Now().Truncate(time.Second),
		CreatedAt:    time.Now().Truncate(time.Second),
	}
	store.Set("u", u)
	var u2 User
	store.Get("u", &u2)
	assert(u == u2)
	ok := store.Get("nop", &u)
	assert(!ok)
}
func TestIncrBy(store KeyValueStore) {
	n := random()
	x := 0
	store.Set("x", n)
	store.Get("x", &x)
	assert(x == n)
	pre := n
	add := random()
	added1 := store.IncrBy("x", add)
	added2 := 0
	store.Get("x", &added2)
	assert(added1 == added2)
	assert(added1 == pre+add)
}
func TestKeyCount(store KeyValueStore) {
	store.FlushAll()
	assert(store.DBSize() == 0)
	key1 := "key1"
	key2 := "key2"
	key3 := "key3"
	store.Set(key1, "aa")
	store.Set(key2, "bb")
	assert(store.Exists(key1))
	assert(store.Exists(key2))
	assert(!store.Exists(key3))
	assert(store.DBSize() == 2)
	store.Del(key3)
	assert(!store.Exists(key3))
	assert(store.DBSize() == 2)
	store.Del(key2)
	assert(!store.Exists(key2))
	assert(store.DBSize() == 1)
	store.Set(key2, "bb")
	assert(store.Exists(key2))
	assert(store.DBSize() == 2)
}
func TestMGetMSet(store KeyValueStore) {
	var keys []string
	localMap := map[string]interface{}{}
	for i := 0; i < 1000; i++ {
		key := "k" + strconv.Itoa(i)
		value := "v" + strconv.Itoa(i*2)
		localMap[key] = value
		keys = append(keys, key)
	}
	store.MSet(localMap)
	v123 := ""
	store.Get("k123", &v123)
	assert(v123 == "v246")
	mgetResult := store.MGet(keys)
	for key, preValue := range localMap {
		var proValue string
		mgetResult.Get(key, &proValue)
		assert(proValue == preValue)
	}
	// check -------------
	// nil key
	// int
	// Set - MGet
	// Speed
	// Set - MGet - Master - Slave
}

func Test3(f func(store KeyValueStore), times int) {
	rand.Seed(time.Now().UnixNano())
	fmt.Println("------- ", runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), " x ", times, " -------")
	for i, store := range stores {
		store.FlushAll()
		start := time.Now()
		for j := 0; j < times; j++ {
			f(store)
		}
		duration := time.Now().Sub(start)
		fmt.Println(names[i], ":", int64(duration/time.Millisecond), "ms")
	}
}

// NewSyncMapServer(GetMasterServerAddress()+":8884", MyServerIsOnMasterServerIP()) のように ISUCON本本では使う
var smMaster KeyValueStore = NewSyncMapServer("127.0.0.1:8080", true)
var smSlave KeyValueStore = NewSyncMapServer("127.0.0.1:8080", false)
var redisWrap KeyValueStore = NewRedisWrapper("127.0.0.1:6379")

var stores = []KeyValueStore{smMaster, smSlave, redisWrap}
var names = []string{"smMaster", "smSlave ", "redis   "}

func main() {
	t := 1
	Test3(TestGetSetInt, t)
	Test3(TestGetSetUser, t)
	Test3(TestIncrBy, t)
	Test3(TestKeyCount, t)
	Test3(TestMGetMSet, 1)
}
