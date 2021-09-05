package main

import (
	"github.com/Muratam/syncmapserver"
	"time"
)

// time.Time は truncateすること。あとpointer型もやめてね
// 大文字のものしか保存されないよ
// 再帰型のスライスも行けるよ
type User struct {
	ID           int64     `json:"id" db:"id"`
	AccountName  string    `json:"account_name" db:"account_name"`
	Address      string    `json:"address,omitempty" db:"address"`
	NumSellItems int       `json:"num_sell_items" db:"num_sell_items"`
	CreatedAt    time.Time `json:"-" db:"created_at"`
}

func assert(cond bool) {
	if !cond {
		panic("assertion failed")
	}
}

func main() {
	// ここで指定した Private IP を持つSyncMapserverがMasterとして、他はSlaveとして起動。
	// 指定しない場合は localhost で起動する
	// syncmapserver.RedisHostPrivateIPAddress = "192.168.2.1"
	smIdToUser := syncmapserver.NewSyncMapServerConnByPort(8885)
	u := User{123, "name", "aaa", 444, time.Now().Truncate(time.Second)}
	smIdToUser.Set("hoge", u) // シリアライズは中で勝手にやってくれる
	var u2 User
	smIdToUser.Get("hoge", &u2)      // 読み込みなので & をつける
	assert(u == u2)                  // 同一になる(time.Now()は .Truncate(time.Second)すること！)
	ok := smIdToUser.Get("piyo", &u) // 存在しないキーなので ok == false になる。
	assert(!ok)
	println("success!!")
}
