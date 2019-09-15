package main

import (
	"fmt"
	"time"
)

type Pos struct {
	X int
	Y int64
	S string
}

// this.Send をしたときに全てここに行く
// うまく書きにくいカスタムなコードを書くことができるよ
func DefaultSendFunction(this *SyncMapServer, buf []byte) []byte {
	return buf
}

func testSyncMapServer() {
	address := "127.0.0.1:8888"
	var masterSyncMapServer = NewMasterOrSlaveSyncMapServer(address, true, DefaultSendFunction)
	var slaveSyncMapServer = NewMasterOrSlaveSyncMapServer(address, false, DefaultSendFunction)
	// # 同期的メソッド
	// pos := Pos{0, 0, ""}
	// masterSyncMapServer.Store("pos", pos)
	// for i := 0; i < 2000; i++ {
	// 	go func() {
	// 		slaveSyncMapServer.StartTransaction(func(tx *SyncMapServerTransaction) {
	// 			var p Pos
	// 			tx.Load("pos", &p)
	// 			p.X += 1
	// 			p.Y += 2
	// 			p.S += "a"
	// 			if len(p.S) > 10 {
	// 				p.S = "0"
	// 			}
	// 			tx.Store("pos", p)
	// 			// if j%100 == 0 {
	// 			fmt.Println(p)
	// 			// }
	// 		})
	// 	}()
	// }
	masterSyncMapServer.Store("x", 0)
	for i := 0; i < 2000; i++ {
		go func() {
			x := slaveSyncMapServer.Add("x", 1)
			fmt.Println(x)
		}()
	}
	for { // 今回は無限に待機
		time.Sleep(1000 * time.Millisecond)
	}
}

func main() {
	testSyncMapServer()
}
