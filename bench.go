package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"
)

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

type User struct {
	ID          int64  `json:"id" db:"id"`
	AccountName string `json:"account_name" db:"account_name"`
	Address     string `json:"address,omitempty" db:"address"`
	// NumSellItems int    `json:"num_sell_items" db:"num_sell_items"`
	// LastBump     time.Time `json:"-" db:"last_bump"`
	// CreatedAt    time.Time `json:"-" db:"created_at"`
}

var localUserMap4000 map[string]interface{}
var keys4000 []string

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
func InitForBenchMGetMSetUser4000() {
	localUserMap4000 = map[string]interface{}{}
	for i := 0; i < 4000; i++ {
		key := randStr()
		localUserMap4000[key] = randUser()
		keys4000 = append(keys4000, key)
	}
}

/////////////////////////////////////////////

func TestGetSetInt(conn KeyValueStoreConn) {
	// int を Get して Set するだけの 一番基本的なやつ
	n := random()
	x := 0
	conn.Set("x", n)
	conn.Get("x", &x)
	assert(x == n)
	conn.Set("y", n*2)
	conn.Get("y", &x)
	assert(x == n*2)
	conn.Get("x", &x)
	assert(x == n)
	ok := conn.Get("nop", &x)
	assert(!ok)
}
func TestGetSetUser(conn KeyValueStoreConn) {
	// userデータ を Get して Set するだけ
	// Pointer 型 は渡せないことに注意。struct in struct は多分大丈夫。
	u := randUser()
	conn.Set("u", u)
	var u2 User
	conn.Get("u", &u2)
	assert(u == u2)
	ok := conn.Get("nop", &u)
	assert(!ok)
}
func TestIncrBy(conn KeyValueStoreConn) {
	n := random()
	x := 0
	conn.Set("x", n)
	conn.Get("x", &x)
	assert(x == n)
	pre := n
	add := random()
	added1 := conn.IncrBy("x", add)
	added2 := 0
	conn.Get("x", &added2)
	assert(added1 == added2)
	assert(added1 == pre+add)
}
func TestKeyCount(conn KeyValueStoreConn) {
	conn.FlushAll()
	assert(conn.DBSize() == 0)
	key1 := "key1"
	key2 := "key2"
	key3 := "key3"
	conn.Set(key1, "aa")
	conn.Set(key2, "bb")
	assert(conn.Exists(key1))
	assert(conn.Exists(key2))
	assert(!conn.Exists(key3))
	assert(conn.DBSize() == 2)
	conn.Del(key3)
	assert(!conn.Exists(key3))
	assert(conn.DBSize() == 2)
	conn.Del(key2)
	assert(!conn.Exists(key2))
	assert(conn.DBSize() == 1)
	conn.Set(key2, "bb")
	assert(conn.Exists(key2))
	assert(conn.DBSize() == 2)
}
func TestMGetMSetString(conn KeyValueStoreConn) {
	var keys []string
	localMap := map[string]interface{}{}
	for i := 0; i < 1000; i++ {
		key := "k" + strconv.Itoa(i)
		value := "v" + strconv.Itoa(i*2)
		localMap[key] = value
		keys = append(keys, key)
	}
	conn.MSet(localMap)
	v8 := ""
	conn.Get("k8", &v8)
	assert(v8 == "v16")
	keys = append(keys, "NOP")
	mgetResult := conn.MGet(keys)
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
func TestMGetMSetUser(conn KeyValueStoreConn) {
	var keys []string
	localMap := map[string]interface{}{}
	for i := 0; i < 1000; i++ {
		key := "k" + strconv.Itoa(i)
		u := randUser()
		localMap[key] = u
		keys = append(keys, key)
	}
	conn.MSet(localMap)
	var v8 User
	conn.Get("k8", &v8)
	assert(v8 == localMap["k8"])
	mgetResult := conn.MGet(keys)
	for key, preValue := range localMap {
		var proValue User
		mgetResult.Get(key, &proValue)
		assert(proValue == preValue)
	}
}
func TestMGetMSetInt(conn KeyValueStoreConn) {
	var keys []string
	localMap := map[string]interface{}{}
	for i := 0; i < 1000; i++ {
		key := "k" + strconv.Itoa(i)
		localMap[key] = i
		keys = append(keys, key)
	}
	conn.MSet(localMap)
	v8 := 0
	conn.Get("k8", &v8)
	assert(v8 == localMap["k8"])
	conn.IncrBy("k8", 1)
	v8 = 0
	conn.Get("k8", &v8)
	assert(v8-1 == localMap["k8"])
	conn.IncrBy("k8", -1)
	mgetResult := conn.MGet(keys)
	for key, preValue := range localMap {
		proValue := 0
		mgetResult.Get(key, &proValue)
		assert(proValue == preValue)
	}
}
func TestParallelTransactionIncr(conn KeyValueStoreConn) {
	conn.Set("a", 0)
	ExecuteImpl(2500, true, 250, func(i int) {
		// Redisは楽観ロックなので成功するまでやる
		// SyncMapServerはロックを取るので成功する
		for !conn.Transaction("a", func(tx KeyValueStoreConn) {
			x := 0
			tx.Get("a", &x)
			tx.Set("a", x+10)
		}) {
		}
	})
	assert(conn.IncrBy("a", 0) == 25000)
}
func BenchMGetMSetUser4000(conn KeyValueStoreConn) {
	conn.MSet(localUserMap4000)
	mgetResult := conn.MGet(keys4000)
	for key, preValue := range localUserMap4000 {
		var proValue User
		mgetResult.Get(key, &proValue)
		assert(proValue.ID == preValue.(User).ID)
	}
}
func BenchMGetMSetStr4000(conn KeyValueStoreConn) {
	localMap := map[string]interface{}{}
	for i := 0; i < 4000; i++ {
		key := keys4000[i]
		localMap[key] = keys4000[i]
	}
	conn.MSet(localMap)
	mgetResult := conn.MGet(keys4000)
	for key, preValue := range localMap {
		var proValue string
		mgetResult.Get(key, &proValue)
		assert(proValue[0] == preValue.(string)[0])
	}
}
func BenchGetSetUser(conn KeyValueStoreConn) {
	k := keys4000[0]
	u := localUserMap4000[keys4000[0]].(User)
	conn.Set(k, u)
	var u2 User
	conn.Get(k, &u2)
	assert(u.ID == u2.ID)
}
func BenchParallelIncryBy(conn KeyValueStoreConn) {
	conn.Set("a", 0)
	Execute(10000, true, func(i int) {
		conn.IncrBy("a", i)
	})
	fmt.Println(conn.IncrBy("a", 0) == 49995000)
}
func BenchParallelUserGetSetPopular(conn KeyValueStoreConn) {
	// 特定のキーにのみアクセス過多
	localMap := map[string]interface{}{}
	for i := 0; i < 400; i++ {
		key := keys4000[i]
		localMap[key] = localUserMap4000[key]
	}
	conn.MSet(localMap)
	ExecuteImpl(10000, true, 1000, func(i int) {
		key := keys4000[i%400]
		n := 200
		if i < n {
			key = keys4000[0]
		}
		for !conn.Transaction(key, func(tx KeyValueStoreConn) {
			proValue := User{}
			tx.Get(key, &proValue)
			preValue := localUserMap4000[key]
			tx.Set(key, preValue)
		}) {
		}
	})
}

func BenchParallelUserGetSet(conn KeyValueStoreConn) {
	Execute(10000, true, func(i int) {
		key := keys4000[i%4000]
		preValue := localUserMap4000[key]
		conn.Set(key, preValue)
		proValue := User{}
		conn.Get(key, &proValue)
		assert(preValue == proValue)
	})
}

// check -------------
// List Push の速度 (use ptr ?)
// Lock を解除したい(RPush / LSet)
// Transactionをチェックしたい

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	// TestMasterSlaveInterpret()
	// gob.Register(User{})
	InitForBenchMGetMSetUser4000()
	// t := 10
	// Test3(TestGetSetInt, t)
	// Test3(TestGetSetUser, t)
	// Test3(TestIncrBy, t)
	// Test3(TestKeyCount, t)
	// Test3(TestMGetMSetString, 1)
	// Test3(TestMGetMSetUser, 1)
	// Test3(TestMGetMSetInt, 1)
	// Test3(TestParallelTransactionIncr, 1)
	// fmt.Println("-----------BENCH----------")
	// Test3(BenchMGetMSetStr4000, 3)
	// Test3(BenchMGetMSetUser4000, 1)
	// Test3(BenchGetSetUser, 4000)
	// TestAverage3(BenchParallelIncryBy, 1) // IncrBy は実装の都合上 Redisのほうがやや速い
	TestAverage3(BenchParallelUserGetSetPopular, 10)
	// TestAverage3(BenchParallelUserGetSet, 1000)
}
