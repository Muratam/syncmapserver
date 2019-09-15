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

// utils
func toStringWithoutLastZero(bytes []byte) string {
	blen := 0
	bMaxLen := len(bytes)
	for ; blen < bMaxLen; blen++ {
		if bytes[blen] == 0 {
			break
		}
	}
	return string(bytes[:blen])
}

// SyncMapServer
// とりあえず string -> byte[] で
type SyncMapServer struct {
	syncmap          sync.Map
	substanceAddress string
	connectNum       MutexInt
}

func (m *SyncMapServer) IsOnThisApp() bool {
	return len(m.substanceAddress) == 0
}
func (m *SyncMapServer) Load(key string) ([]byte, bool) {
	val, ok := m.syncmap.Load(key)
	if !ok {
		return []byte(""), false
	}
	return val.([]byte), true
}
func (m *SyncMapServer) Store(key string, value []byte) {
	m.syncmap.Store(key, value)
}
func NewSyncMapServer() *SyncMapServer {
	result := &SyncMapServer{}
	result.substanceAddress = ""
	return result
}

// SyncMapServer
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

func (this *SyncMapServer) serverImpl(conn *net.Conn) {
	defer (*conn).Close()
	buf := readAll(conn)
	// 一連の要求がまとめて送られてくる。その中では整合性が保てていて欲しい。
	command := string(buf[:4])
	content := toStringWithoutLastZero(buf[4:])
	// fmt.Println("Serve:", command, content)
	if strings.Compare(command, "GET ") == 0 {
		loaded, _ := this.Load(content)
		conn.Write(loaded)
		return
	} else if strings.Compare(command, "SET ") == 0 {
		splitted := strings.SplitN(content, "\n\n", 2) // WARN: キーに \n\nがあったら死ぬ
		if len(splitted) != 2 {
			conn.Write([]byte("x"))
		} else {
			this.Store(splitted[0], []byte(splitted[1]))
			conn.Write([]byte("o"))
		}
		return
	} else {

	}

}

func (this *SyncMapServer) StartSyncMapServer(port int) {
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
		go this.serverImpl(&conn)
	}
}

func (this *SyncMapServer) ConnectByClient(f func(conn net.Conn)) { // WARN: conn ptr?
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
		this.ConnectByClient(f)
		return
	}
	f(conn)
	conn.Close()
	this.connectNum.Dec()
}

func testConnectByClient(substanceAddress string) {
	var clientSyncMap = NewSyncMapServer()
	clientSyncMap.substanceAddress = substanceAddress
	for i := 0; i < 1; i++ {
		go func() {
			clientSyncMap.ConnectByClient(func(conn net.Conn) {
				x := ""
				for i := 0; i < 1025; i++ {
					x += "a"
				}
				buf := []byte(x)
				conn.Write(buf)
			})
			// x := 0
			// clientSyncMap.ConnectByClient(func(conn net.Conn) {
			// 	buf := []byte("GET iikanji")
			// 	conn.Write(buf)
			// 	n, err := conn.Read(buf)
			// 	if err != nil {
			// 		panic(err)
			// 	}
			// 	x, _ = strconv.Atoi(string(buf[:n]))
			// })
			// fmt.Println(x)
			// x += 1
			// clientSyncMap.ConnectByClient(func(conn net.Conn) {
			// 	buf := []byte("SET iikanji\n\n" + strconv.FormatInt(int64(x), 10))
			// 	conn.Write(buf)
			// })
		}()
	}
}

func testSyncMapServer() {
	var serverSyncMap = NewSyncMapServer()
	go serverSyncMap.StartSyncMapServer(8888)
	time.Sleep(10 * time.Millisecond)
	serverSyncMap.Store("iikanji", []byte("0"))
	go testConnectByClient("127.0.0.1:8888")
	for { // 通常はGojiとかのサブとして使うので無限に待機
		time.Sleep(1000 * time.Millisecond)
	}
}

func main() {
	testSyncMapServer()
}
