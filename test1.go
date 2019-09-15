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
	wg := sync.WaitGroup{}
	address := "127.0.0.1:8888"
	var masterSyncMapServer = NewMasterOrSlaveSyncMapServer(address, true, DefaultSendCustomFunction)
	// var slaveSyncMapServer = NewMasterOrSlaveSyncMapServer(address, false, DefaultSendCustomFunction)
	// // inc Struct test
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
	// // inc test
	// masterSyncMapServer.Store("x", 0)
	// for i := 0; i < 2000; i++ {
	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()
	// 		x := masterSyncMapServer.Add("x", 1)
	// 		fmt.Println(x)
	// 	}()
	// }
	// // list test
	masterSyncMapServer.InitList("x")
	for i := 0; i < 2000; i++ {
		wg.Add(1)
		j := i
		go func() {
			defer wg.Done()
			masterSyncMapServer.AppendList("x", j)
			l := masterSyncMapServer.LenOfList("x")
			// Update / load
			fmt.Println(j+1, l)
		}()
	}
	wg.Wait()
	// all, _ := masterSyncMapServer.SyncMap.Load("x")
	// for _, a := range all.([][]byte) {
	// 	x := 0
	// 	DecodeFromBytes(a, &x)
	// 	fmt.Println(x)
	// }

}

func main() {
	testSyncMapServer()
}
