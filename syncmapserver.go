package main

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

const maxClientConnectionNum = 15

// MutexInt
type MutexInt struct {
	mutex sync.Mutex
	val   int
}

func (this *MutexInt) Set(value int) {
	this.mutex.Lock()
	this.val = value
	this.mutex.Unlock()
}
func (this *MutexInt) Get() int {
	this.mutex.Lock()
	val := this.val
	this.mutex.Unlock()
	return val
}
func (this *MutexInt) Inc() int {
	this.mutex.Lock()
	this.val += 1
	val := this.val
	this.mutex.Unlock()
	return val
}
func (this *MutexInt) Dec() int {
	this.mutex.Lock()
	this.val -= 1
	val := this.val
	this.mutex.Unlock()
	return val
}

// SyncMapServer

func StartSyncMapServer(port int) {
	listen, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(port))
	defer listen.Close()
	if err != nil {
		panic(err)
	}
	var countSum MutexInt
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("Server:", err)
			continue
		}
		go func() {
			defer conn.Close()
			for { // EOFまで読む
				readMax := 1024
				buf := make([]byte, readMax)
				readBufNum, err := conn.Read(buf)
				if err != nil {
					if err == io.EOF {
						return
					}
					panic(err)
				}
				fmt.Println(countSum.Inc())
				// fmt.Print("S:", string(buf))
				if readBufNum < readMax { // たぶんあってるはず
					return
				}
			}
		}()
	}
}

func testConnect(port int) {
	var connectFunc func()
	var connectNum MutexInt
	connectFunc = func() {
		for connectNum.Get() > maxClientConnectionNum {
			time.Sleep(time.Duration(100+rand.Intn(400)) * time.Nanosecond)
		}
		connectNum.Inc()
		conn, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
		if err != nil {
			fmt.Println("Client", connectNum.Get(), err)
			if conn != nil {
				conn.Close()
			}
			time.Sleep(1 * time.Millisecond)
			connectNum.Dec()
			connectFunc()
			return
		}
		buf := []byte("write")
		conn.Write(buf)
		conn.Close()
		connectNum.Dec()
	}
	for i := 0; i < 1000; i++ {
		go connectFunc()
	}
}

func testSyncMapServer() {
	go StartSyncMapServer(8888)
	time.Sleep(10 * time.Millisecond)
	go testConnect(8888)
	for { // 通常はGojiとかのサブとして使うので無限に待機
		time.Sleep(1000 * time.Millisecond)
	}
}

func main() {
	testSyncMapServer()
}
