package main
// NOTE: 仮の実装.だいたい合ってると思うが適当なISUCONのでチェックしたい
// vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv このコードを埋め込む
import (
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/shamaton/msgpack"
)

func loadAndDecode(path string, x interface{}) {
	encoded, err := ioutil.ReadFile(path)
	if err != nil {
		log.Panic("cannot read data...")
	}
	msgpack.Decode(encoded, x)
}

var x []X = func()[]X {
	var result X[]
	loadAndDecode("./hoge.data",&result) // NOTE: データが保存されているパスを書く
	return result
}()

// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
// xo : DB を そのままGo のコードにできるかもしれないが...
//   https://nnao45.hatenadiary.com/entry/2018/12/19/101834
//   http://tdoc.info/blog/2016/07/06/xo.html
// DBに接続してSELECTのSQLを実行して全て取得し、オンメモリにしたコードを吐く
// 更新されないデータにはまず間違いなく適応できる
// 以下の2種類の目的があるが,1.はMySQLを見れば済む話なので
//   1. データの様子を眺めるためにデータをGoにする
//   2. 膨大なデータでもオンメモリにするのが大事なのでバイナリファイルに吐き出す
func dump(db *sqlx.DB) {
	db, _ := sql.Open("postgres", "dbname=example sslmode=disable") 	// NOTE: 接続する方法を書く
	xs := []X{}                                     // NOTE: type X を適当な型に置き換える
	err := dbx.Select(&xs, "SELECT * FROM `users`") // NOTE: 取りたいSQL文を書く
	if err != nil {
		panic(err)
	}
	encodeAndSave("./hoge.data",&xs)  // NOTE: 保存する名前を書く
}

// 以下は実装用
func main() {
	dump()
}
func encodeAndSave(path string,x interface{}) {
	d, _ := msgpack.Encode(x)
	file, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	file.Write(encode(xs))
}
