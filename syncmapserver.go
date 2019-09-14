package main

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// SyncMap で頑張るサーバー。Goアプリの上で動かす。
// 1: 1台目のアプリで動かすので1台目->1台目のロスtcpロスがなくて速いはず
// 2: トランザクションが容易
// 3: OnMemory (再起動可能かは未だ)
// 4: MySQL からのデータの移動を容易にしたいね
// Initilize も可能にしておきたい

func StartSyncMapServer(port int) {
	listen, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(port))
	defer listen.Close()
	if err != nil {
		panic(err)
	}
	for {
		conn, err := listen.Accept()
		defer conn.Close()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go func() {
			for { // EOFまで読む
				buf := make([]byte, 1024)
				_, err := conn.Read(buf)
				if err != nil {
					if err == io.EOF {
						return
					}
					panic(err)
				}
				fmt.Print("S:", string(buf))
			}
		}()
	}
}
func testConnect(port int) {
	for {
		conn, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
		defer conn.Close()
		if err != nil {
			fmt.Println(err)
			continue
		}
		buf := []byte("write")
		conn.Write(buf)
		time.Sleep(100 * time.Nanosecond)
	}
}

func main() {
	go StartSyncMapServer(8888)
	time.Sleep(10 * time.Millisecond)
	go testConnect(8888)
	time.Sleep(50000 * time.Millisecond) // 通常はサブとして使うので...
}
