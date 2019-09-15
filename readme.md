# Go SyncMap Server
- SyncMap でトランザクションを頑張るサーバー。Goアプリの上で動かす。

1. 1台目のアプリで動かすので1台目->1台目のロスtcpロスがなくて速いはず
2. トランザクションが容易
3. OnMemory (再起動可能かは未だ)
4. MySQL からのデータの移動を容易にしたいね

# TODO
- `ulimit -n` の上限までコネクションプール
  - `sudo ulimit -n 6049`
  - `sudo sysctl -w kern.ipc.somaxconn=1024`
- Initilize も可能にしておきたい
- Redis / MySQL で同様の処理をしたときの速度を取っておきたい
  - トランザクションも考えてね
- Connection Pool
  - TCP接続の時間を減らせるけど...
