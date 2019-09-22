package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"sort"
	"strconv"
	"time"
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
	func() {
		getNow := func() time.Time { return time.Now().Truncate(time.Second) }
		type Tree struct {
			X  int
			Y  int
			T  time.Time
			TR []Tree
		}
		var eqTree func(a, b Tree) bool
		eqTree = func(a, b Tree) bool {
			if a.X != b.X || a.Y != b.Y || a.T != b.T || len(a.TR) != len(b.TR) {
				return false
			}
			for i := 0; i < len(a.TR); i++ {
				if !eqTree(a.TR[i], b.TR[i]) {
					return false
				}
			}
			return true
		}
		smMaster.FlushAll()
		src := Tree{X: random(), Y: random(), T: getNow(), TR: []Tree{Tree{X: random(), Y: random(), T: getNow()}}}
		dst1 := Tree{}
		dst2 := Tree{}
		smSlave.Set("k1", src)
		smMaster.Get("k1", &dst1)
		smSlave.Get("k1", &dst2)
		assert(eqTree(src, dst1))
		assert(eqTree(src, dst2))
	}()
	func() { // INSERT
		smSlave.FlushAll()
		n := 10000
		Execute(n, true, func(i int) {
			smSlave.Insert(i)
			smMaster.Insert(i)
		})
		keys := smMaster.AllKeys()
		keyInts := make([]int, len(keys))
		for i, key := range keys {
			x, _ := strconv.Atoi(key)
			keyInts[i] = x
		}
		sort.IntSlice(keyInts).Sort()
		for i := 0; i < n*2; i++ {
			assert(i+1 == keyInts[i])
		}
	}()
	fmt.Println("-------  Master Slave Test Passed  -------")
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
	assert(len(conn.AllKeys()) == 2)
	conn.Del(key3)
	assert(!conn.Exists(key3))
	assert(conn.DBSize() == 2)
	assert(len(conn.AllKeys()) == 2)
	conn.Del(key2)
	assert(!conn.Exists(key2))
	assert(conn.DBSize() == 1)
	assert(len(conn.AllKeys()) == 1)
	conn.Set(key2, "bb")
	assert(conn.Exists(key2))
	assert(conn.DBSize() == 2)
	assert(len(conn.AllKeys()) == 2)
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
func TestLRangeInt(conn KeyValueStoreConn) {
	conn.FlushAll()
	key := "a"
	n := 10
	for i := 0; i < n; i++ {
		conn.RPush(key, i)
	}
	gots := conn.LRange(key, 0, -1)
	for i := 0; i < n; i++ {
		x := -1
		gots.Get(i, &x)
		assert(x == i)
	}
	assert(gots.Len() == n)
	gots2 := conn.LRange(key, -1, -1)
	assert(gots2.Len() == 1)
	gots2 = conn.LRange(key, -3, -1)
	assert(gots2.Len() == 3)
	gots2 = conn.LRange(key, 0, 4)
	assert(gots2.Len() == 5)
	assert(conn.LLen(key) == n)
	for i := 0; i < n; i++ {
		x := -1
		conn.LPop(key, &x)
		assert(x == i)
	}
	assert(conn.LLen(key) == 0)
	for i := 0; i < n; i++ {
		conn.RPush(key, i)
	}
	assert(conn.LLen(key) == n)
	for i := 0; i < n; i++ {
		x := -1
		conn.RPop(key, &x)
		assert(x == n-i-1)
	}
	assert(conn.LLen(key) == 0)
}
func BenchListUser(conn KeyValueStoreConn) {
	conn.FlushAll()
	n := 10000
	assert(n%2 == 0)
	key := "lkey"
	for i := 0; i < n; i += 2 {
		u1 := localUserMap4000[keys4000[i%4000]]
		u2 := localUserMap4000[keys4000[(i+1)%4000]]
		x := conn.RPush(key, u1, u2)
		assert(i == x-1)
		u3x := localUserMap4000[keys4000[(i/2)%4000]]
		u3y := User{}
		conn.LIndex(key, (i/2)%4000, &u3y)
		assert(u3x == u3y)
	}
	assert(n == conn.LLen(key))
	for i := 0; i < n; i++ {
		u1 := localUserMap4000[keys4000[(100+i)%4000]]
		conn.LSet(key, i, u1)
		u2 := User{}
		conn.LIndex(key, i, &u2)
		assert(u1 == u2)
	}
	assert(n == conn.LLen(key))
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
func TestParallelList(conn KeyValueStoreConn) {
	// Redisは楽観ロックなので成功するまでやる
	// SyncMapServerはロックを取るので成功する
	ExecuteImpl(2500, true, 250, func(i int) {
		for !conn.Transaction("a", func(tx KeyValueStoreConn) { tx.IncrBy("a", 1) }) {
		}
	})
	assert(conn.IncrBy("a", 0) == 2500)
	conn.Set("a", 0)
	ExecuteImpl(2500, true, 250, func(i int) {
		conn.IncrBy("a", 1)
	})
	assert(conn.IncrBy("a", 0) == 2500)
	conn.FlushAll()
	ExecuteImpl(2500, true, 250, func(i int) {
		for !conn.Transaction("a", func(tx KeyValueStoreConn) {
			l := tx.LLen("a")
			tx.RPush("a", 1)
			if l > 0 {
				tx.RPop("a", nil)
			}
		}) {
		}
	})
	assert(conn.LLen("a") == 1)
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
	InitForBenchMGetMSetUser4000()
	TestMasterSlaveInterpret()
	t := 10
	Test3(TestGetSetInt, t)
	Test3(TestGetSetUser, t)
	Test3(TestIncrBy, t)
	Test3(TestKeyCount, t)
	Test3(TestLRangeInt, t)
	Test3(TestParallelList, 1)
	Test3(TestMGetMSetString, 1)
	Test3(TestMGetMSetUser, 1)
	Test3(TestMGetMSetInt, 1)
	Test3(TestParallelTransactionIncr, 1)
	fmt.Println("-----------BENCH----------")
	Test3(BenchMGetMSetStr4000, 1)
	Test3(BenchMGetMSetUser4000, 1)
	Test3(BenchGetSetUser, 4000)
	TestAverage3(BenchListUser, 1)
	TestAverage3(BenchParallelIncryBy, 1)
	TestAverage3(BenchParallelUserGetSetPopular, 1)
	TestAverage3(BenchParallelUserGetSet, 1000)
}
