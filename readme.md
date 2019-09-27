# Go SyncMap Server

- Go の sync.Map を使ったKey-Value型のオンメモリのDB。
  - MasterはGoアプリの上で動かすので1台分のTCPコストがゼロ！
  - Slave側はRedisと同程度の速度。
- 実装されているメソッドの殆どはRedisと互換性があるため、切り替えが容易。Redisラッパーも同梱。

- `node sqlmap.js ../*.go > sqlmap.json && dot sqlmap.dot -Tpng > sqlmap.png`
  - Insert() 時に、IDは未定なことに注意

# 使い分け
- Redisの楽観ロックに対して、こちらは悲観的ロック。
  - MySQLと同じロック方式なのでコードを置き換えやすい。
    - ISUCONの場合は同じキーにも大量の更新がある場合が多いので悲観ロックのほうがたぶんよい。
    - そうでなければRedisと併用すればよいという話。
  - IsLocked() が書ける。 Insert() も書ける(こちらはIDが全て連続しているなら).
    - キー毎にロックされているかをトランザクション開始前に確認可能
- こちらのListは内部的にはスライスによる実装(Redisは双方向連結リスト)。

# 定期的にバックアップを取れるので再起動試験も安心
- 読み込み時にバックアップファイルがあればそれを読み込む
- Initlaize関数を設定するとSQL内の初期化データもローカルファイルにキャッシュできる

# すべてのデータを []byte で保存するため速い
- Goアプリからはinterface{}で型の変換がすぐにできる。
- ただしこれは RedisWrapper の方でも同じなので速度に差は無い。
- シリアライズもMessagePackで爆速。Gobの約5倍速

# ISUCONでの使用時のヒント
- https://github.com/Muratam/isucon9q/blob/master/postapi.go
  - 特定のキーのみのロック(+1人目のトランザクションが成功したら終了) は postBuy()
  - 要求があってから初めて接続を開始するので複数台でも起動順序は問われない。

# 扱える命令
```go
// Normal Command
Get(key string, value interface{}) bool // ptr (キーが無ければ false)
Set(key string, value interface{})
MGet(keys []string) MGetResult     // 改めて Get するときに ptr . N+1対策
MSet(store map[string]interface{}) // 先に対応Mapを作りそれをMSet. N+1対策
Exists(key string) bool
Del(key string)
IncrBy(key string, value int) int
DBSize() int       // means key count
AllKeys() []string // get all keys
FlushAll()
// Transaction (キーをロックしているのでこの中の tx を使って値を読み書きしてね)
Transaction(key string, f func(tx KeyValueStoreConn)) (isok bool)
TransactionWithKeys(keys []string, f func(tx KeyValueStoreConn)) (isok bool)
// List 関連 (こういうのはRedisの高機能のデータ構造のほうが使いそう)
RPush(key string, values ...interface{}) int // Push後の最後の要素の index を返す
LLen(key string) int
LIndex(key string, index int, value interface{}) bool // ptr (キーが無ければ false)
LPop(key string, value interface{}) bool              // ptr (キーが無ければ false)
RPop(key string, value interface{}) bool              // ptr (キーが無ければ false)
LSet(key string, index int, value interface{})
LRange(key string, startIndex, stopIncludingIndex int) LRangeResult // ptr (0,-1 で全て取得可能) (負数の場合はPythonと同じような処理(stopIncludingIndexがPythonより1多い)) [a,b,c][0:-1] はPythonでは最後を含まないがこちらは含む
// ISUCONで初期化の負荷を軽減するために使う
Initialize()
```

以下はSyncMapServerのみ

```go
IsLocked(key string) // Redisには存在しないがSyncMapServerには(悲観ロックなので)存在する
Insert(value interface{}) // str(DBSize()+1)のキーに(そのキーをロックして)挿入
```

# 使い方

- User Struct をGet/Setする例

```go
// ここで指定した Private IP を持つSyncMapserverがMasterとして、他はSlaveとして起動。
const RedisHostPrivateIPAddress = "172.24.122.185"
var isMasterServerIP = MyServerIsOnMasterServerIP()
var idToUserServer = NewSyncMapServerConn(GetMasterServerAddress()+":8884", isMasterServerIP)

func main(){
  u := randUser() // テスト用のランダムに User Struct を作成する関数
  idToUserServer.Set("hoge", u) // シリアライズは中で勝手にやってくれる
  var u2 User
  idToUserServer.Get("hoge", &u2) // 読み込みなので & をつける
  assert(u == u2) // 同一になる(time.Now()は .Truncate(time.Second)すること！)
  ok := conn.Get("piyo", &u) // 存在しないキーなので ok == false になる。
  assert(!ok)
}
```

- User Struct をMGet/MSetする例

```go
// ここで指定した Private IP を持つSyncMapserverがMasterとして、他はSlaveとして起動。
func main(){
  conn := idToUserServer
  // MSet は map[string]interface{}{} を作ってそれを渡すことで実行する。
  var keys []string
  localMap := map[string]interface{}{}
  for i := 0; i < 1000; i++ {
    key := "k" + strconv.Itoa(i)
    localMap[key] = randUser()
    keys = append(keys, key)
  }
  conn.MSet(localMap)
  // 保存できている。
  var v8 User
  conn.Get("k8", &v8)
  assert(v8 == localMap["k8"])
  // MGetResult 型で帰ってくる。Len() / Get() が使える。
  mgetResult := conn.MGet(keys)
  for key, preValue := range localMap {
    var proValue User
    mgetResult.Get(key, &proValue)
    assert(proValue == preValue)
  }
}
```

- キーをロックするTransaction
```go
func main(){
  conn.Set("a", 0)
  for i := 0 ; i < 2500 ; i ++ {
    go func(){
      // SyncMapServerはロックを取るので成功するのでTransaction()は必ずtrueになる。
      // Redisは楽観ロックなので成功するまでやる。Tranasction()を失敗するとfalse(その場合結果は反映されない).
      // 存在しないキーへのロックも大丈夫だよ
      for !conn.Transaction("a", func(tx KeyValueStoreConn) {
        x := 0
        tx.Get("a", &x)
        tx.Set("a", x+10)
      }) {
      }
    }()
  })
  // トランザクションをしたけど実は普通は IncrByを使えばいいテストでした。
  assert(conn.IncrBy("a", 0) == 25000)
}
```

- 互換性を持ったままRedisにする例
```go
// 0番DBの指定したIPのところのRedisにつなぐ
var idToUserServer = NewRedisWrapper("127.0.0.1", 0)

func main(){
  // あとは SyncMapServer のものと全く同じコードでよい。(楽観ロックが異なるTransaction以外は)
  u := randUser()
  idToUserServer.Set("hoge", u)
  var u2 User
  idToUserServer.Get("hoge", &u2)
  assert(u == u2)
  ok := conn.Get("piyo", &u)
  assert(!ok)
}

```


## ベンチマークと動作テスト

- `$ go run *.go` . ローカルのRedisとMasterSlaveのSyncMap

```
-------  main.TestGetSetInt  x  1000  -------
smMaster : 2 ms
smSlave  : 185 ms
redis    : 285 ms
-------  main.TestGetSetUser  x  1000  -------
smMaster : 30 ms
smSlave  : 129 ms
redis    : 204 ms
-------  main.TestIncrBy  x  1000  -------
smMaster : 1 ms
smSlave  : 138 ms
redis    : 195 ms
-------  main.TestKeyCount  x  1000  -------
smMaster : 6 ms
smSlave  : 574 ms
redis    : 1637 ms
-------  main.TestLRangeInt  x  1000  -------
smMaster : 45 ms
smSlave  : 1482 ms
redis    : 3259 ms
-------  main.TestParallelList  x  1  -------
smMaster : 18 ms
smSlave  : 623 ms
redis    : 3199 ms
-------  main.TestMGetMSetString  x  1  -------
smMaster : 3 ms
smSlave  : 2 ms
redis    : 2 ms
-------  main.TestMGetMSetUser  x  1  -------
smMaster : 31 ms
smSlave  : 36 ms
redis    : 34 ms
-------  main.TestMGetMSetInt  x  1  -------
smMaster : 3 ms
smSlave  : 2 ms
redis    : 2 ms
-------  main.TestParallelTransactionIncr  x  1  -------
smMaster : 4 ms
smSlave  : 275 ms
redis    : 2000 ms
-----------BENCH----------
-------  main.BenchMGetMSetStr4000  x  1  -------
smMaster : 14 ms
smSlave  : 32 ms
redis    : 30 ms
-------  main.BenchMGetMSetUser4000  x  1  -------
smMaster : 24 ms
smSlave  : 50 ms
redis    : 37 ms
-------  main.BenchGetSetUser  x  4000  -------
smMaster : 27 ms
smSlave  : 285 ms
redis    : 421 ms
-------  main.BenchListUser  x  1  -------
smMaster : 57 ms
smSlave  : 1122 ms
redis    : 1615 ms
-------  main.BenchParallelIncryBy  x  1  -------
smMaster : 14 ms
smSlave  : 108 ms
redis    : 183 ms
-------  main.BenchParallelUserGetSetPopular  x  1  -------
smMaster : 32 ms
smSlave  : 475 ms
redis    : 939 ms
-------  main.BenchParallelUserGetSet  x  1  -------
smMaster : 29 ms
smSlave  : 251 ms
redis    : 275 ms
```

- Masterサーバーの操作は速い。オーバーヘッドがないから当然。
- 並列にすると速い。 だいたいSlaveとRedisの速度は同じくらいだが、チューニングしたのでちょっとだけSlaveのほうが速い。
- ISUCONに近いように同じキーへのアクセス(+更新)が多めに設定されているベンチは1.5~2倍くらい速度が違う。
- 全てのキーに満遍なくアクセスが有る main.BenchParallelUserGetSet でも同程度の速度がでているので嬉しい！

# ロックについて
- RedisもSyncMapServerもロールバックはサポートされない。
- [GET...] -> [SET...] の操作のうち,途中でやめたいことがある場合は, [SET...] より前にやめておけば問題は発生しない。
  - つまり、 [SET...] を DB の操作が全て終わった後(= Commit() の直前)に行えば問題ない。
- keys は中でソートされるので、(DAGができるので)デッドロックは発生しないはず。
- Redis は楽観ロックなので,この中の関数が楽観ロックに失敗した場合に成功するまで実行され続けることに注意。
- Redis版では Set 系操作の後に Get 系操作があったらエラーがでるようになってる。
  - isok: SyncMapServerの場合は必ず成功する。/ Redis の場合は失敗するかもしれない(その場合はデータの変更が発生しない) => Commit()の直前なので Rollback()すればよい。
  -  DB.Update() -> redis.Transaction.Set(){} -> (Commit() / RollBack())
