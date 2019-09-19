package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"

	_ "net/http/pprof"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"time"
)

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
	result := "あいうえおかきくけこ"
	for i := 0; i < 100; i++ {
		result += strconv.Itoa(random())
	}
	return result
}
func randUser() User {
	return User{
		ID:          int64(random()),
		AccountName: randStr(),
		Address:     randStr(),
		// NumSellItems: random(),
		// LastBump:     time.Now().Truncate(time.Second),
		// CreatedAt:    time.Now().Truncate(time.Second),
	}
}
func Execute(times int, isParallel bool, f func(i int)) {
	if isParallel {
		maxGoroutineNum := 50
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

type User struct {
	ID          int64  `json:"id" db:"id"`
	AccountName string `json:"account_name" db:"account_name"`
	Address     string `json:"address,omitempty" db:"address"`
	// NumSellItems int    `json:"num_sell_items" db:"num_sell_items"`
	// LastBump     time.Time `json:"-" db:"last_bump"`
	// CreatedAt    time.Time `json:"-" db:"created_at"`
}

func TestGetSetInt(store KeyValueStoreConnWithTransaction) {
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
func TestGetSetUser(store KeyValueStoreConnWithTransaction) {
	// userデータ を Get して Set するだけ
	// Pointer 型 は渡せないことに注意。struct in struct は多分大丈夫。
	u := randUser()
	store.Set("u", u)
	var u2 User
	store.Get("u", &u2)
	assert(u == u2)
	ok := store.Get("nop", &u)
	assert(!ok)
}
func TestIncrBy(store KeyValueStoreConnWithTransaction) {
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
func TestKeyCount(store KeyValueStoreConnWithTransaction) {
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
func TestMGetMSetString(store KeyValueStoreConnWithTransaction) {
	var keys []string
	localMap := map[string]interface{}{}
	for i := 0; i < 1000; i++ {
		key := "k" + strconv.Itoa(i)
		value := "v" + strconv.Itoa(i*2)
		localMap[key] = value
		keys = append(keys, key)
	}
	store.MSet(localMap)
	v8 := ""
	store.Get("k8", &v8)
	assert(v8 == "v16")
	keys = append(keys, "NOP")
	mgetResult := store.MGet(keys)
	vNop := ""
	ok := mgetResult.Get("NOP", &vNop)
	assert(!ok)
	assert(vNop == "")
	for key, preValue := range localMap {
		var proValue string
		ok = mgetResult.Get(key, &proValue)
		assert(proValue == preValue)
		assert(ok)
	}
}

func TestMGetMSetUser(store KeyValueStoreConnWithTransaction) {
	var keys []string
	localMap := map[string]interface{}{}
	for i := 0; i < 1000; i++ {
		key := "k" + strconv.Itoa(i)
		u := randUser()
		localMap[key] = u
		keys = append(keys, key)
	}
	store.MSet(localMap)
	var v8 User
	store.Get("k8", &v8)
	assert(v8 == localMap["k8"])
	mgetResult := store.MGet(keys)
	for key, preValue := range localMap {
		var proValue User
		mgetResult.Get(key, &proValue)
		assert(proValue == preValue)
	}
}
func TestMGetMSetInt(store KeyValueStoreConnWithTransaction) {
	var keys []string
	localMap := map[string]interface{}{}
	for i := 0; i < 1000; i++ {
		key := "k" + strconv.Itoa(i)
		localMap[key] = i
		keys = append(keys, key)
	}
	store.MSet(localMap)
	v8 := 0
	store.Get("k8", &v8)
	assert(v8 == localMap["k8"])
	store.IncrBy("k8", 1)
	v8 = 0
	store.Get("k8", &v8)
	assert(v8-1 == localMap["k8"])
	store.IncrBy("k8", -1)
	mgetResult := store.MGet(keys)
	for key, preValue := range localMap {
		proValue := 0
		mgetResult.Get(key, &proValue)
		assert(proValue == preValue)
	}
}
func TestMasterSlaveInterpret() {
	func() {
		smMaster.FlushAll()
		u := randUser()
		smSlave.Set("k1", u)
		var u1 User
		smMaster.Get("k1", &u1)
		assert(u == u1)
		var u2 User
		smSlave.Get("k1", &u2)
		assert(u == u2)
	}()
	func() {
		smMaster.FlushAll()
		u := randUser()
		smMaster.Set("k1", u)
		var u1 User
		smMaster.Get("k1", &u1)
		assert(u == u1)
		var u2 User
		smSlave.Get("k1", &u2)
		assert(u == u2)
	}()
	func() {
		smMaster.FlushAll()
		u := randUser()
		smSlave.MSet(map[string]interface{}{"k1": u, "k2": u, "k3": u})
		var u1 User
		smMaster.Get("k1", &u1)
		assert(u == u1)
		var u2 User
		smSlave.Get("k2", &u2)
		assert(u == u2)
	}()
	fmt.Println("-------  Master Slave Test Passed  -------")
}

var localUserMap4000 map[string]interface{}
var keys4000 []string

func InitForBenchMGetMSetUser4000() {
	localUserMap4000 = map[string]interface{}{}
	for i := 0; i < 4000; i++ {
		key := randStr()
		localUserMap4000[key] = randUser()
		keys4000 = append(keys4000, key)
	}
}
func BenchMGetMSetUser4000(store KeyValueStoreConnWithTransaction) {
	store.MSet(localUserMap4000)
	mgetResult := store.MGet(keys4000)
	for key, preValue := range localUserMap4000 {
		var proValue User
		mgetResult.Get(key, &proValue)
		assert(proValue.ID == preValue.(User).ID)
	}
}
func BenchMGetMSetStr4000(store KeyValueStoreConnWithTransaction) {
	localMap := map[string]interface{}{}
	for i := 0; i < 4000; i++ {
		key := keys4000[i]
		localMap[key] = keys4000[i]
	}
	store.MSet(localMap)
	mgetResult := store.MGet(keys4000)
	for key, preValue := range localMap {
		var proValue string
		mgetResult.Get(key, &proValue)
		assert(proValue[0] == preValue.(string)[0])
	}
}
func BenchGetSetUser(store KeyValueStoreConnWithTransaction) {
	k := keys4000[0]
	u := localUserMap4000[keys4000[0]].(User)
	store.Set(k, u)
	var u2 User
	store.Get(k, &u2)
	assert(u.ID == u2.ID)
}
func BenchParallelIncryBy(store KeyValueStoreConnWithTransaction) {
	store.Set("a", 0)
	Execute(10000, true, func(i int) {
		store.IncrBy("a", i)
	})
	fmt.Println(store.IncrBy("a", 0) == 49995000)
}

func BenchParallelUserGetSet(store KeyValueStoreConnWithTransaction) {
	Execute(10000, true, func(i int) {
		key := keys4000[i%4000]
		preValue := localUserMap4000[key]
		store.Set(key, preValue)
		proValue := User{}
		store.Get(key, &proValue)
		assert(preValue == proValue)
	})
}

// check -------------
// List Push の速度 (use ptr ?)
// Lock を解除したい(RPush / LSet)
// Transactionをチェックしたい

func Test3(f func(store KeyValueStoreConnWithTransaction), times int) (milliSecs []int64) {
	rand.Seed(time.Now().UnixNano())
	fmt.Println("------- ", runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), " x ", times, " -------")
	for i, store := range stores {
		store.FlushAll()
		start := time.Now()
		for j := 0; j < times; j++ {
			f(store)
		}
		duration := time.Now().Sub(start)
		milliSecs = append(milliSecs, int64(duration/time.Millisecond))
		fmt.Println(names[i], ":", milliSecs[i], "ms")
	}
	return milliSecs
}
func TestAverage3(f func(store KeyValueStoreConnWithTransaction), times int) {
	milliSecs := make([]int64, len(stores))
	for n := 1; n < times; n++ {
		resMilliSecs := Test3(f, 1)
		fmt.Println("AVERAGE:")
		for i := 0; i < len(milliSecs); i++ {
			milliSecs[i] += resMilliSecs[i]
			fmt.Println("  ", names[i], ":", milliSecs[i]/int64(n), "ms")
		}
	}
}

// NewSyncMapServer(GetMasterServerAddress()+":8884", MyServerIsOnMasterServerIP()) のように ISUCON本本では使う
var smMasterInstance = NewSyncMapServer("127.0.0.1:8080", true)
var smSlaveInstance = NewSyncMapServer("127.0.0.1:8080", false)
var redisWrapInstance = NewRedisWrapper("127.0.0.1:6379")
var smMaster = smMasterInstance.GetConn()
var smSlave = smSlaveInstance.GetConn()
var redisWrap = redisWrapInstance.GetConn()

var stores = []KeyValueStoreConnWithTransaction{smMaster, smSlave, redisWrap}
var names = []string{"smMaster", "smSlave ", "redis   "}

// var stores = []KeyValueStoreConnWithTransaction{smMaster, redisWrap}
// var names = []string{"smMaster", "redis   "}
// var stores = []KeyValueStoreConnWithTransaction{smSlave}
// var names = []string{"smSlave "}
// var stores = []KeyValueStoreConnWithTransaction{smMaster}
// var names = []string{"smMaster"}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	InitForBenchMGetMSetUser4000()
	t := 10
	Test3(TestGetSetInt, t)
	Test3(TestGetSetUser, t)
	Test3(TestIncrBy, t)
	Test3(TestKeyCount, t)
	Test3(TestMGetMSetString, 1)
	Test3(TestMGetMSetUser, 1)
	Test3(TestMGetMSetInt, 1)
	TestMasterSlaveInterpret()
	fmt.Println("-----------BENCH----------")
	Test3(BenchMGetMSetStr4000, 3)
	Test3(BenchMGetMSetUser4000, 1)
	Test3(BenchGetSetUser, 4000)
	TestAverage3(BenchParallelIncryBy, 1) // NOTE: IncrBy は実装が悪いので Redisのほうがやや速い
	TestAverage3(BenchParallelUserGetSet, 1000)
}
