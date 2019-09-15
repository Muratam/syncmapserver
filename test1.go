package main

import (
	"fmt"
	"sync"
)

type Pos struct {
	X int
	Y int64
	S string
}

// this.Send をしたときに全てここに行く
// うまく書きにくいカスタムなコードを書くことができるよ

func testSyncMapServer() {
	address := "127.0.0.1:8888"
	var masterSyncMapServer = NewMasterOrSlaveSyncMapServer(address, true, DefaultSendCustomFunction)
	// var slaveSyncMapServer = NewMasterOrSlaveSyncMapServer(address, false, DefaultSendCustomFunction)
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
	wg := sync.WaitGroup{}
	for i := 0; i < 2000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			x := masterSyncMapServer.Add("x", 1)
			fmt.Println(x)
		}()
	}
	wg.Wait()
}

func main() {
	testSyncMapServer()
}
