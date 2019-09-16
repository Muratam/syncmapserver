# Go SyncMap Server
- SyncMap でトランザクションを頑張るサーバー。Goアプリの上で動かす。

1. 1台目のアプリで動かすので1台目->1台目のTCPロスがなくて速い
  (テストではTCPを経由しても(多分どちらもGoなので[]byteの変換が容易なため)Redisよりも速い)
1. トランザクションが可能(lock / unlock)
1. 内部的には[]byteで保存しているが、メソッドを生やしているので型の変換が簡単。
1. 大量のデータの初期化が容易(Redisは一括で送信するのが大変)
1. list も扱える
1. OnMemory (30秒毎にバックアップファイルを作成してくれるので安心)
1. 複数台の起動順序はOK (connection に失敗したら panic(というかerr)))

# やるだけ
1. TODO: MySQL からのデータの移動を容易にしたいね


# 現在できないこと
1. Mapのキーの種類数取得
1. listの全てを一括取得
1. 一部のキーだけをLock(複数台syncmapserverを立てれば良い)
1. あるトランザクション中の、トランザクションをしない操作による値の変更の制限


# NOTE
- `ulimit -n` の上限までコネクションプール
  - `sudo ulimit -n 6049`
  - `sudo sysctl -w kern.ipc.somaxconn=1024`
- Initilize も可能にしておきたい
- Connection Pool
  - TCP接続の時間を減らせるけど...
