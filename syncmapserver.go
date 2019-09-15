package main

import (
	"bytes"
	"encoding/gob"
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

func EncodeToBytes(x interface{}) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(x); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// NOTE: 変更できるようにpointer型で受け取ること
func DecodeFromBytes(bytes_ []byte, x interface{}) {
	var buf bytes.Buffer
	buf.Write(bytes_)
	dec := gob.NewDecoder(&buf)
	err := dec.Decode(x)
	if err != nil {
		panic(err)
	}
}

// SyncMapServer
// とりあえず string -> byte[] で
type SyncMapServer struct {
	SyncMap          sync.Map
	substanceAddress string
	connectNum       MutexInt
	mutex            sync.Mutex
	SendImpl         func(this *SyncMapServer, buf []byte) []byte
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
	this.SendImpl = func(this *SyncMapServer, buf []byte) []byte { return buf }
	return this
}
func newSlaveSyncMapServer(substanceAddress string) *SyncMapServer {
	this := &SyncMapServer{}
	this.substanceAddress = substanceAddress
	this.SendImpl = func(this *SyncMapServer, buf []byte) []byte { return buf }
	return this
}
func NewMasterOrSlaveSyncMapServer(
	substanceAddress string,
	isMaster bool,
	SendImpl func(this *SyncMapServer, buf []byte) []byte) *SyncMapServer {

	if isMaster {
		port, _ := strconv.Atoi(strings.Split(substanceAddress, ":")[1])
		result := newMasterSyncMapServer(port)
		result.SendImpl = SendImpl
		return result
	} else {
		result := newSlaveSyncMapServer(substanceAddress)
		result.SendImpl = SendImpl
		return result
	}
}

// SyncMapServer
func (this *SyncMapServer) sendBySlave(f func() []byte, force bool) []byte {
	if !force {
		for this.connectNum.Get() > maxSyncMapServerConnectionNum {
			time.Sleep(time.Duration(100+rand.Intn(400)) * time.Nanosecond)
		}
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
		return this.sendBySlave(f, force)
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

// 自身の SyncMapからLoad
// NOTE: 変更できるようにpointer型で受け取ること
func (this *SyncMapServer) LoadDirect(key string, res interface{}) bool {
	value, ok := this.SyncMap.Load(key)
	if ok {
		DecodeFromBytes(value.([]byte), res)
	}
	return ok
}

// 自身の SyncMapにStore
func (this *SyncMapServer) StoreDirect(key string, value interface{}) {
	encoded := EncodeToBytes(value)
	this.SyncMap.Store(key, encoded)
}

var syncMapCustomCommand = []byte("CT") // custom
var syncMapLoadCommand = []byte("LD")   // load
var syncMapStoreCommand = []byte("ST")  // store
var syncMapLockAllCommand = []byte("LOCK")
var syncMapUnlockAllCommand = []byte("UNLOCK")
var syncMapIncCommand = []byte("INC")            // +1 (as number) TODO:
var syncMapDecCommand = []byte("DEC")            // -1 (as number) TODO:
var syncMapLockKeyCommand = []byte("LOCK_K")     // lock a key     TODO:
var syncMapUnlockKeyCommand = []byte("UNLOCK_K") // unlock a key   TODO:
type SyncMapServerTransaction struct {
	server *SyncMapServer
}

func join(input [][]byte) []byte {
	return EncodeToBytes(input)
}
func split(input []byte) [][]byte {
	result := make([][]byte, 0)
	DecodeFromBytes(input, &result)
	return result
}

// 生のbyteを送信
func (this *SyncMapServer) send(f func() []byte, force bool) []byte {
	if this.IsOnThisApp() {
		return this.interpretWrapFunction(f())
	} else {
		return this.sendBySlave(f, force)
	}
}

func (this *SyncMapServer) sendImpl(f func() []byte, force bool) []byte {
	return this.send(func() []byte {
		return join([][]byte{
			syncMapCustomCommand,
			f(),
		})
	}, force)
}
func (this *SyncMapServer) Send(f func() []byte) []byte {
	return this.sendImpl(f, false)
}
func (this *SyncMapServerTransaction) Send(f func() []byte) []byte {
	return this.server.sendImpl(f, true)
}

// NOTE: 変更できるようにpointer型で受け取ること
// Masterなら直に、SlaveならTCPでつないで実行
func (this *SyncMapServer) loadImpl(key string, res interface{}, force bool) bool {
	if this.IsOnThisApp() {
		return this.LoadDirect(key, res)
	} else { // やっていき
		loadedBytes := this.send(func() []byte {
			return join([][]byte{
				syncMapLoadCommand,
				[]byte(key),
			})
		}, force)
		if len(loadedBytes) == 0 {
			return false
		}
		DecodeFromBytes(loadedBytes, res)
		return true
	}
}
func (this *SyncMapServer) Load(key string, res interface{}) bool {
	return this.loadImpl(key, res, false)
}
func (this *SyncMapServerTransaction) Load(key string, res interface{}) bool {
	return this.server.loadImpl(key, res, true)
}

// Masterなら直に、SlaveならTCPでつないで実行
func (this *SyncMapServer) storeImpl(key string, value interface{}, force bool) {
	if this.IsOnThisApp() {
		this.StoreDirect(key, value)
	} else { // やっていき
		this.send(func() []byte {
			return join([][]byte{
				syncMapStoreCommand,
				[]byte(key),
				EncodeToBytes(value),
			})
		}, force)
	}
}
func (this *SyncMapServer) Store(key string, value interface{}) {
	this.storeImpl(key, value, false)
}
func (this *SyncMapServerTransaction) Store(key string, value interface{}) {
	this.server.storeImpl(key, value, true)
}
func (this *SyncMapServer) StartTransaction(f func(this *SyncMapServerTransaction)) {
	if this.IsOnThisApp() {
		this.mutex.Lock()
	} else { // やっていき
		this.send(func() []byte {
			return join([][]byte{
				syncMapLockAllCommand,
			})
		}, false)
	}
	var tx SyncMapServerTransaction
	tx.server = this
	f(&tx)
	tx.endTransaction()
}

func (this *SyncMapServerTransaction) endTransaction() {
	if this.server.IsOnThisApp() {
		this.server.mutex.Unlock()
	} else { // やっていき
		this.server.send(func() []byte {
			return join([][]byte{
				syncMapUnlockAllCommand,
			})
		}, true)
	}
}

func (this *SyncMapServer) interpretWrapFunction(buf []byte) []byte {
	ss := split(buf)
	if len(ss) < 1 {
		panic(nil)
	}
	command := ss[0]
	if bytes.Compare(command, syncMapCustomCommand) == 0 {
		return this.SendImpl(this, ss[1])
	} else if bytes.Compare(command, syncMapLoadCommand) == 0 {
		key := string(ss[1])
		value, ok := this.SyncMap.Load(key)
		if !ok {
			return []byte("")
		}
		return value.([]byte)
	} else if bytes.Compare(command, syncMapStoreCommand) == 0 {
		key := string(ss[1])
		value := ss[2]
		this.SyncMap.Store(key, value)
		return []byte("")
	} else if bytes.Compare(command, syncMapLockAllCommand) == 0 {
		this.mutex.Lock() // 永遠にロックしたらやだなー
		return []byte("")
	} else if bytes.Compare(command, syncMapUnlockAllCommand) == 0 {
		this.mutex.Unlock()
		return []byte("")
	} else {
		panic(nil)
	}
}
