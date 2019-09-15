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
func SendImpl(this *SyncMapServer, buf []byte) []byte {
	// this.LockAll()
	// x, _ := this.SyncMap.Load("sum")
	// this.SyncMap.Store("sum", x.(int)+1)
	// this.UnlockAll()
	// fmt.Println(x)
	// command := string(buf[:4])
	// // fmt.Println("Serve:", command, content)
	// if strings.Compare(command, "GET ") == 0 {
	// 	loaded, _ := this.Load(content)
	// 	conn.Write(loaded)
	// 	return
	// } else if strings.Compare(command, "SET ") == 0 {
	// 	splitted := strings.SplitN(content, "\n\n", 2) // WARN: キーに \n\nがあったら死ぬ
	// 	if len(splitted) != 2 {
	// 		conn.Write([]byte("x"))
	// 	} else {
	// 		this.Store(splitted[0], []byte(splitted[1]))
	// 		conn.Write([]byte("o"))
	// 	}
	// 	return
	// } else {	}
	// 雑に echo
	return buf
}

func testSyncMapServer() {
	address := "127.0.0.1:8888"
	var masterSyncMapServer = NewMasterOrSlaveSyncMapServer(address, true, SendImpl)
	var slaveSyncMapServer = NewMasterOrSlaveSyncMapServer(address, false, SendImpl)
	// # 同期的メソッド
	pos := Pos{0, 0, ""}
	masterSyncMapServer.Store("pos", pos)
	for i := 0; i < 2000; i++ {
		go func() {
			slaveSyncMapServer.StartTransaction(func(tx *SyncMapServerTransaction) {
				var p Pos
				tx.Load("pos", &p)
				p.X += 1
				p.Y += 2
				p.S += "a"
				if len(p.S) > 10 {
					p.S = "0"
				}
				tx.Store("pos", p)
				// if j%100 == 0 {
				fmt.Println(p)
				// }
			})
		}()
	}
	for { // 今回は無限に待機
		time.Sleep(1000 * time.Millisecond)
	}
}

// 実際はこんな感じで使いたい
// var globalSyncMapServer = NewMasterOrSlaveSyncMapServer("127.0.0.1:9000", false)

func main() {
	testSyncMapServer()
}
