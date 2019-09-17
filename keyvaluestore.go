package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

// Redis と SyncMapSercer の違いを吸収
type KeyValueStoreCore interface { // ptr は参照を着けてLoadすることを示す
	// Normal Command
	Get(key string, value interface{}) bool // ptr (キーが無ければ false)
	Set(key string, value interface{})
	// MSet(keys []string, values []interface{}) // ptr
	// MGet(keys []string, values []interface{})
	// Exists(key string) bool
	// Del(key string)
	// IncrBy(key string, value int) int
	// KeysCount() int
	// Keys() []string TODO:
	FlushAll()
	// InitList(key string)                     // SyncMapServerの方は必要
	// RPush(key string, value interface{}) int // Push後の 自身の index を返す
	// LLen(key string) int
	// LIndex(key string, index int, value interface{}) bool // ptr (キーが無ければ false)
	// LSet(key string, index int, value interface{})
	// LRange(key string, start, stop int, values []interface{}) // ptr(0,-1 で全て取得可能) TODO:
}

// KeysCount : Redisに無い
// InitList : Redisには無い

type KeyValueStore interface {
	KeyValueStoreCore
	// StartTransaction(f func(tx *KeyValueStoreCore))
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
	return strconv.Itoa(random())
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
}
func TestGetSetUser(store KeyValueStore) {
	// userデータ を Get して Set するだけ
	type User struct {
		ID          int64  `json:"id" db:"id"`
		AccountName string `json:"account_name" db:"account_name"`
		// HashedPassword []byte    `json:"-" db:"hashed_password"`
		Address      string    `json:"address,omitempty" db:"address"`
		NumSellItems int       `json:"num_sell_items" db:"num_sell_items"`
		LastBump     time.Time `json:"-" db:"last_bump"`
		CreatedAt    time.Time `json:"-" db:"created_at"`
	}
	u := User{
		ID:          int64(random()),
		AccountName: randStr(),
		// HashedPassword: []byte(randStr()),
		Address:      randStr(),
		NumSellItems: random(),
		LastBump:     time.Now().Truncate(time.Second),
		CreatedAt:    time.Now().Truncate(time.Second),
	}
	store.Set("u", u)
	var u2 User
	store.Get("u", &u2)
	// assert(bytes.Compare(u.HashedPassword, u2.HashedPassword) == 0)
	assert(u == u2)
	// fmt.Print(u, u2)
}

// NewSyncMapServer(GetMasterServerAddress()+":8884", MyServerIsOnMasterServerIP()) のように ISUCON本本では使う
var smMaster KeyValueStore = NewSyncMapServer("127.0.0.1:8080", true)
var smSlave KeyValueStore = NewSyncMapServer("127.0.0.1:8080", false)
var redisWrap KeyValueStore = NewRedisWrapper("127.0.0.1:6379")

func Test3(f func(store KeyValueStore)) {
	rand.Seed(time.Now().UnixNano())
	stores := []KeyValueStore{smMaster, smSlave, redisWrap}
	names := []string{"smMaster", "smSlave", "redis"}
	for i, store := range stores {
		store.FlushAll()
		f(store)
		fmt.Print(names[i], "ok. ")
	}
	fmt.Println("")
}

func main() {
	Test3(TestGetSetInt)
	Test3(TestGetSetUser)
}
