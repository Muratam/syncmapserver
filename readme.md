# Go SyncMap Server

- SyncMap でRedis的なことを頑張るサーバー。Goアプリの上で動かす。
- ISUCONに特化した最適化をすることで、すごい速度を実現。
- Master と Slave に分かれており、Masterを動かしているGoアプリはTCPを経由せずにデータを扱える。
- キーバリューストア型のDB。
- 同梱のRedisWrapperも KeyValueStore interface を持っているので一瞬で切り替えることが可能、もしものときにも安心。


## Redisよりも 速い
- On Memory なのでそもそも速い
  - 30秒毎にバックアップファイルを作成してくれるのでレギュレーション的にも安心！
    -  読み込み時にバックアップファイルがあればそれを読み込む
- Goアプリ上で動かすので速い
  - Redisだと同じサーバーでもRedisへTCPで通信する必要があるが、その必要がない。
- すべてのデータを []byte で保存するため速い
  - Goアプリからはinterface{}で型の変換がすぐにできる。
- 大量のデータの初期化が速い
  - Redisは一括で送信しないと莫大な時間がかかるが、こちらはMasterサーバーで初期化することでオーバーヘッドなしに初期化できる。

# Redis の主要な機能が使える
- トランザクション(Lock / Unlock)が可能
  - 全体をロックする他に、個別のキーだけをロックすることも可能。
    - TODO: 一人がロック中に他の人が書き換えられるのは注意.
  - キー毎にロックされているかをトランザクション開始前に確認可能
- list(=保存順序を気にしないデータの配列) も扱える
- 全てのキーの要素数も確認可能
- MULTIGET / MULTISET があるので N+1問題にも対応可能


# やるだけ
1. TODO: goコードの中からSQLを吸い出したい(過去のISUCON全てで読めるようになっていれば良さそう)
1. TODO: 一つのキーに保存された list の 全てを一括取得も実装しておきたい
1. join / split はもっと高速化できそう(EncodeAt/DecodeByをちゃんとjoin/splitに置き換える)
- METHOD:
  - 本家は RPush が一度に複数送信できるっぽい
# ISUCONでの使用時のヒント
- https://github.com/Muratam/isucon9q/blob/nouser/postapi.go
  - DBからのSQLでの読み込み は initializeUsersDB()
  - 特定のキーのみのロック(+1人目のトランザクションが成功したら終了) は postBuy()
  - 要求があってから初めて接続を開始するので複数台でも起動順序は問われない。


# ベンチマークと動作テスト
1. syncmap / rediswrapper の速度の比較と動作テスト keyvaluestore.go でしています
