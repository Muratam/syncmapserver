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

func testSyncMapServer() {
	address := "127.0.0.1:8888"
	var masterSyncMapServer = NewMasterOrSlaveSyncMapServer(address, true,
		func(this *SyncMapServer, buf []byte) []byte {
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
		})
	var slaveSyncMapServer = NewMasterOrSlaveSyncMapServer(address, false, masterSyncMapServer.interpret)
	// masterSyncMapServer.Store("key", 10)
	// var x int
	// masterSyncMapServer.Load("key", &x)
	// fmt.Println(x)
	// # 同期的メソッド
	pos := Pos{0, 0, ""}
	masterSyncMapServer.Store("pos", pos)
	for i := 0; i < 1000; i++ {
		var p Pos
		masterSyncMapServer.Load("pos", &p)
		slaveSyncMapServer.Load("pos", &p)
		p.X += 1
		p.Y += 2
		p.S += "a"
		masterSyncMapServer.Store("pos", p)
		fmt.Println(p.Y)
	}
	/*
		// 直で保存する
		masterSyncMapServer.SyncMap.Store("sum", 0)
		go func() {
			// var slaveSyncMapServer = NewMasterOrSlaveSyncMapServer(address, false, masterSyncMapServer.interpret)
			for i := 0; i < 10000; i++ {
				go func() {
					masterSyncMapServer.Send(func() []byte {
						return []byte("increment sum 1")
					})
					// fmt.Println(string(result), len(result))
					// 	buf := []byte("GET iikanji")
					// 	x, _ = strconv.Atoi(string(buf[:n]))
					// 	buf := []byte("SET iikanji\n\n" + strconv.FormatInt(int64(x), 10))
					// })
				}()
			}
		}()
	*/
	for { // 今回は無限に待機
		time.Sleep(1000 * time.Millisecond)
	}
}

// 実際はこんな感じで使いたい
// var globalSyncMapServer = NewMasterOrSlaveSyncMapServer("127.0.0.1:9000", false)

func main() {
	testSyncMapServer()
}
