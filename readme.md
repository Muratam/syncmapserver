# Go SyncMap Server

- SyncMap でRedis的なことを頑張るサーバー。Goアプリの上で動かす。
- ISUCONに特化した最適化をすることで、すごい速度を実現。
- Master と Slave に分かれており、Masterを動かしているGoアプリはTCPを経由せずにデータを扱える。
- キーバリューストア型のDB。
- 同梱のRedisWrapperも KeyValueStore interface を持っているので一瞬で切り替えることが可能。状況に応じて使い分けやすい。
- きちんとロックする。楽観ロックではない。
  - IsLocked() コマンドが使える。

## Redis並の速度が出る
- Goアプリ上で動かすので速い
  - Redisだと同じサーバーでもRedisへTCPで通信する必要があるが、その必要がないところが特徴。
  - 例えば 3台 の場合、単純計算で 1.5倍速！
- On Memory なので Redis並の速度が出る
  - 30秒毎にバックアップファイルを作成してくれるのでレギュレーション的にも安心！
  -  読み込み時にバックアップファイルがあればそれを読み込む
- すべてのデータを []byte で保存するため速い
  - Goアプリからはinterface{}で型の変換がすぐにできる。
  - ただしこれは RedisWrapper の方でも同じなので速度に差は無い
- 大量のデータの初期化が速い
  - Redisは一括で送信しないと莫大な時間がかかるが、こちらはMasterサーバーで初期化することでオーバーヘッドなしに初期化できる。

## Redis の主要な機能が使える
- トランザクション(Lock / Unlock)が可能
  - 全体をロックする他に、個別のキーだけをロックすることも可能。
  - キー毎にロックされているかをトランザクション開始前に確認可能
- list(=保存順序を気にしないデータの配列) も扱える
- 全てのキーの要素数も確認可能
- MULTIGET / MULTISET があるので N+1問題にも対応可能


## ベンチマークと動作テスト

- syncmap / rediswrapper の速度の比較と動作テスト keyvaluestore.go でしています
- User struct を作り、それをむちゃくちゃな回数 Get / Set しまくるコード

```
codegen + single
  smMaster : 21 ms
  smSlave  : 713 ms
  redis    : 1074 ms
codegen + parallel  (50並列)
  smMaster : 17 ms
  smSlave  : 317 ms
  redis    : 317 ms
gob + parallel  (50並列)
  smMaster : 194 ms
  smSlave  : 472 ms
  redis    : 515 ms
```

- Masterサーバーの操作は速い。オーバーヘッドがないから当然。
- エンコード・デコードにかなり時間がかかる。可能なら手間だが codegen を使うべき。
- 並列にすると速い。 Redis の速度と Slave の速度はほぼ同じになる。
  - これは予想通り。SyncMapServerの良点は1台分がTCPしなくてよいところなので


# ISUCONでの使用時のヒント
- https://github.com/Muratam/isucon9q/blob/nouser/postapi.go
  - DBからのSQLでの読み込み は initializeUsersDB()
  - 特定のキーのみのロック(+1人目のトランザクションが成功したら終了) は postBuy()
  - 要求があってから初めて接続を開始するので複数台でも起動順序は問われない。

# DBと併用する際のトランザクションのメモ
- ロールバックはサポートされない。
- [GET...] -> [SET...] の操作のうち,途中でやめたいことがある場合は, [SET...] より前にやめておけば問題は発生しない。
  - つまり、 [SET...] を DB の操作が全て終わった後(= Commit() の直前)に行えば問題ない。
- SyncMapServer は通常どおりのロック. 長く専有しているものがあると大変だがそもそも直列操作なので仕方ない。
- keys は中でソートされるので、(DAGができるので)デッドロックは発生しないはず。
- Redis は楽観ロックなので,この中の関数が楽観ロックに失敗した場合に成功するまで実行され続けることに注意。
- Redis版では Set 系操作の後に Get 系操作があったらエラーがでるようになってる。
	- isok: SyncMapServerの場合は必ず成功する。/ Redis の場合は失敗するかもしれない(その場合はデータの変更が発生しない) => Commit()の直前なので Rollback()すればよい。
	-  DB.Update() -> redis.Transaction.Set(){} -> (Commit() / RollBack())

# やるだけ

1. TODO: トランザクションの強化(テストの充実)
1. TODO: list の速度検証 -> LRange / LPop / RPop
1. TODO: MessagePackが失敗するものを探す(Sliceとかポインターとか)
1. TODO: goコードの中からSQLを吸い出したい(過去のISUCON全てで読めるようになっていれば良さそう)
1. TODO: さらにさらにDBの中身ををGoのコードに簡単に吸い出せるようにしたい。
1. BackUpの検証
Readを1回に / 使い方 / UserInterface
