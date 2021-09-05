# 動作サンプル

## 通常サンプル

`go run sample.go`

## ベンチマークサンプル

`go run bench.go benchutil.go`


## バックアップサンプル

データの保存＆読み込みテスト

`go run backuptest.go benchutil.go store`

既存データの読み込みテスト
(store 前にやるとデータがなくて落ちる)

`go run backuptest.go benchutil.go`
