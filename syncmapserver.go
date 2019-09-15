package main

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxSyncMapServerConnectionNum = 15

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

// bytes utils
func readAll(conn *net.Conn) []byte {
	readMax := 1024
	bufAll := make([]byte, readMax)
	alreadyReadAll := false
	if true { // 1回目だけ特別にすることで []byte のコピーを削減
		readBufNum, err := (*conn).Read(bufAll)
		if err != nil {
			if err == io.EOF {
				alreadyReadAll = true
			} else {
				panic(err)
			}
		}
		if readBufNum < readMax {
			alreadyReadAll = true
			bufAll = bufAll[:readBufNum]
		}
	}
	for !alreadyReadAll { // EOFまで読む
		buf := make([]byte, readMax)
		readBufNum, err := (*conn).Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		bufAll = append(bufAll, buf[:readBufNum]...)
		if readBufNum < readMax { // 読み込んだ数が少ないのでもうこれ以上読み込む必要がない
			break
		}
	}
	return bufAll
}

// SyncMapServer
// とりあえず string -> byte[] で
type SyncMapServer struct {
	SyncMap          sync.Map
	substanceAddress string
	connectNum       MutexInt
	mutex            sync.Mutex
	Interpret        func(this *SyncMapServer, buf []byte) []byte
}

func (this *SyncMapServer) interpretWrapFunction(buf []byte) []byte {
	return this.Interpret(this, buf)
}

func newMasterSyncMapServer(port int) *SyncMapServer {
	this := &SyncMapServer{}
	this.substanceAddress = ""
	go func() {
		listen, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(port))
		defer listen.Close()
		if err != nil {
			panic(err)
		}
		for {
			conn, err := listen.Accept()
			if err != nil {
				fmt.Println("Server:", err)
				continue
			}
			go func() {
				conn.Write(this.interpretWrapFunction(readAll(&conn)))
				conn.Close()
			}()
		}
	}()
	// 起動終了までちょっと時間がかかるかもしれないので待機しておく
	time.Sleep(10 * time.Millisecond)
	// 何も設定しなければecho
	this.Interpret = func(this *SyncMapServer, buf []byte) []byte { return buf }
	return this
}
func newSlaveSyncMapServer(substanceAddress string) *SyncMapServer {
	this := &SyncMapServer{}
	this.substanceAddress = substanceAddress
	this.Interpret = func(this *SyncMapServer, buf []byte) []byte { return buf }
	return this
}

// SyncMapServer
func (this *SyncMapServer) sendBySlave(f func() []byte) []byte {
	for this.connectNum.Get() > maxSyncMapServerConnectionNum {
		time.Sleep(time.Duration(100+rand.Intn(400)) * time.Nanosecond)
	}
	this.connectNum.Inc()
	conn, err := net.Dial("tcp", this.substanceAddress)
	if err != nil {
		fmt.Println("Client", this.connectNum.Get(), err)
		if conn != nil {
			conn.Close()
		}
		time.Sleep(1 * time.Millisecond)
		this.connectNum.Dec()
		return this.sendBySlave(f)
	}
	conn.Write(f())
	result := readAll(&conn)
	conn.Close()
	this.connectNum.Dec()
	return result
}

// public methods
func (this *SyncMapServer) IsOnThisApp() bool {
	return len(this.substanceAddress) == 0
}
func (this *SyncMapServer) LockAll() {
	this.mutex.Lock()
}
func (this *SyncMapServer) UnlockAll() {
	this.mutex.Unlock()
}
func (this *SyncMapServer) Send(f func() []byte) []byte {
	if this.IsOnThisApp() {
		return this.interpretWrapFunction(f())
	} else {
		return this.sendBySlave(f)
	}
}
func NewMasterOrSlaveSyncMapServer(substanceAddress string, isMaster bool) *SyncMapServer {
	if isMaster {
		port, _ := strconv.Atoi(strings.Split(substanceAddress, ":")[1])
		return newMasterSyncMapServer(port)
	} else {
		return newSlaveSyncMapServer(substanceAddress)
	}
}

/*
func (this *SyncMapServer) Load(key interface{}) (value interface{}, ok bool) {
	if this.IsOnThisApp() {
		return this.SyncMap.Load(key)
	} else { // やっていき？
		return this.SyncMap.Load(key)
	}
}
func (this *SyncMapServer) Store(key, value interface{}) {
	if this.IsOnThisApp() {
		this.SyncMap.Store(key, value)
	} else { // やっていき？
		this.SyncMap.Store(key, value)
	}
}
func (m *SyncMapServer) LoadBytesDirect(key string) ([]byte, bool) {
	val, ok := m.SyncMap.Load(key)
	if !ok {
		return []byte(""), false
	}
	return val.([]byte), true
}
func (m *SyncMapServer) StoreBytesDirect(key string, value []byte) {
	m.SyncMap.Store(key, value)
}
*/

func testSyncMapServer() {
	address := "127.0.0.1:8888"
	var masterSyncMapServer = NewMasterOrSlaveSyncMapServer(address, true)
	// 直で保存する
	masterSyncMapServer.SyncMap.Store("sum", 0)
	// Interpret を変更すればいい感じになる。
	masterSyncMapServer.Interpret = func(this *SyncMapServer, buf []byte) []byte {
		this.LockAll()
		x, _ := this.SyncMap.Load("sum")
		this.SyncMap.Store("sum", x.(int)+1)
		this.UnlockAll()
		fmt.Println(x)
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
	go func() {
		var slaveSyncMapServer = NewMasterOrSlaveSyncMapServer(address, false)
		slaveSyncMapServer.Interpret = masterSyncMapServer.Interpret
		for i := 0; i < 10000; i++ {
			go func() {
				slaveSyncMapServer.Send(func() []byte {
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
	for { // 今回は無限に待機
		time.Sleep(1000 * time.Millisecond)
	}
}

// 実際はこんな感じで使いたい
// var globalSyncMapServer = NewMasterOrSlaveSyncMapServer("127.0.0.1:9000", false)

func main() {
	testSyncMapServer()
}
