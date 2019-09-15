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
type SyncMapServer struct {
	SyncMap          sync.Map // string -> (byte[] | byte[][])
	keyCount         MutexInt
	substanceAddress string
	connectNum       MutexInt
	mutex            sync.Mutex
	// mutexMap             sync.Map
	MySendCustomFunction func(this *SyncMapServer, buf []byte) []byte
}

func DefaultSendCustomFunction(this *SyncMapServer, buf []byte) []byte {
	return buf // echo server
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
	this.MySendCustomFunction = func(this *SyncMapServer, buf []byte) []byte { return buf }
	return this
}
func newSlaveSyncMapServer(substanceAddress string) *SyncMapServer {
	this := &SyncMapServer{}
	this.substanceAddress = substanceAddress
	this.MySendCustomFunction = func(this *SyncMapServer, buf []byte) []byte { return buf }
	return this
}
func NewMasterOrSlaveSyncMapServer(
	substanceAddress string,
	isMaster bool,
	MySendCustomFunction func(this *SyncMapServer, buf []byte) []byte) *SyncMapServer {

	if isMaster {
		port, _ := strconv.Atoi(strings.Split(substanceAddress, ":")[1])
		result := newMasterSyncMapServer(port)
		result.MySendCustomFunction = MySendCustomFunction
		return result
	} else {
		result := newSlaveSyncMapServer(substanceAddress)
		result.MySendCustomFunction = MySendCustomFunction
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

// TODO: transaction中にno transactionなものが値を変えてくる可能性が十分にある
//       特に STORE / DELETE はやっかい。だが、たいていこれらはTransactionがついているはずなのでそこまで注意をしなくてもよいのではないか
var syncMapCustomCommand = []byte("CT")          // custom
var syncMapLoadCommand = []byte("LD")            // load
var syncMapStoreCommand = []byte("ST")           // store
var syncMapLockAllCommand = []byte("LOCK")       // start transaction WARN: no lock timeout
var syncMapUnlockAllCommand = []byte("UNLOCK")   // end transaction
var syncMapAddCommand = []byte("ADD")            // add value
var syncMapExistsKeyCommand = []byte("EXISTS")   // check if exists key WARN: no test
var syncMapDeleteCommand = []byte("DEL")         // delete WARN: no test
var syncMapLengthCommand = []byte("LEN")         // key count TODO:
var syncMapLockKeyCommand = []byte("LOCK_K")     // lock a key     TODO:
var syncMapUnlockKeyCommand = []byte("UNLOCK_K") // unlock a key   TODO:
// TODO: list (内部的に([]byte ではなく [][]byte として保存しているので) Store / Load は使えない)
var syncMapInitListCommand = []byte("INIT_LIST")     // init list
var syncMapAppendListCommand = []byte("APPEND_LIST") // append value to list(空でも可能)
var syncMapLenListCommand = []byte("LEN_LIST")       // len of list
var syncMapIndexListCommand = []byte("INDEX_LIST")   // get value from list
var syncMapUpdateListCommand = []byte("UPDATE_LIST") // update value at index
// WARN: no GET_ALL
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

func (this *SyncMapServer) sendCustomImpl(f func() []byte, forceDirect, forceConnect bool) []byte {
	if forceDirect || this.IsOnThisApp() {
		return this.MySendCustomFunction(this, f())
	} else {
		return this.send(func() []byte {
			return join([][]byte{
				syncMapCustomCommand,
				f(),
			})
		}, forceConnect)
	}
}
func (this *SyncMapServer) SendCustom(f func() []byte) []byte {
	return this.sendCustomImpl(f, false, false)
}
func (this *SyncMapServerTransaction) SendCustom(f func() []byte) []byte {
	return this.server.sendCustomImpl(f, false, true)
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

// Masterなら直に、SlaveならTCPでつないで実行
func (this *SyncMapServer) deleteImpl(key string, forceDirect, forceConnection bool) {
	if forceDirect || this.IsOnThisApp() {
		this.SyncMap.Delete(key)
	} else {
		this.send(func() []byte {
			return join([][]byte{
				syncMapDeleteCommand,
				[]byte(key),
			})
		}, forceConnection)
	}
}
func (this *SyncMapServer) Delete(key string) {
	this.deleteImpl(key, false, false)
}
func (this *SyncMapServerTransaction) Delete(key string) {
	this.server.deleteImpl(key, false, true)
}

// Masterなら直に、SlaveならTCPでつないで実行
func (this *SyncMapServer) exitstsImpl(key string, forceDirect, forceConnection bool) bool {
	if forceDirect || this.IsOnThisApp() {
		_, ok := this.SyncMap.Load(key)
		return ok
	} else {
		encoded := this.send(func() []byte {
			return join([][]byte{
				syncMapExistsKeyCommand,
				[]byte(key),
			})
		}, forceConnection)
		ok := false
		DecodeFromBytes(encoded, &ok)
		return ok
	}
}
func (this *SyncMapServer) Exists(key string) bool {
	return this.exitstsImpl(key, false, false)
}
func (this *SyncMapServerTransaction) Exists(key string) bool {
	return this.server.exitstsImpl(key, false, true)
}

// Masterなら直に、SlaveならTCPでつないで実行
func (this *SyncMapServer) addImpl(key string, value int, forceDirect bool) int {
	if forceDirect || this.IsOnThisApp() {
		this.mutex.Lock()
		x := 0
		this.LoadDirect(key, &x)
		x += value
		this.StoreDirect(key, x)
		this.mutex.Unlock()
		return x
	} else { // やっていき
		x := this.send(func() []byte {
			return join([][]byte{
				syncMapAddCommand,
				[]byte(key),
				EncodeToBytes(value),
			})
		}, false)
		result := 0
		DecodeFromBytes(x, &result)
		return result
	}
}
func (this *SyncMapServer) Add(key string, value int) int {
	return this.addImpl(key, value, false)
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

// 配列を初期化する
func (this *SyncMapServer) initListImpl(key string, forceDirect, forceConnection bool) {
	if forceDirect || this.IsOnThisApp() {
		// WARN: 速度が気になれば *[][]byte にすることを検討する
		this.SyncMap.Store(key, make([][]byte, 0))
	} else {
		this.send(func() []byte {
			return join([][]byte{
				syncMapInitListCommand,
				[]byte(key),
			})
		}, forceConnection)
	}
}
func (this *SyncMapServer) InitList(key string) {
	this.initListImpl(key, false, false)
}
func (this *SyncMapServerTransaction) InitList(key string) {
	this.server.initListImpl(key, false, true)
}

// index を返す
func (this *SyncMapServer) appendListImpl(key string, value interface{}, forceDirect, forceConnection bool) int {
	if forceDirect || this.IsOnThisApp() {
		this.mutex.Lock() // TODO: クソ雑ロック(特定のキーだけロックしておきたい)
		elist, ok := this.SyncMap.Load(key)
		if !ok {
			this.SyncMap.Store(key, make([][]byte, 0))
			elist, _ = this.SyncMap.Load(key)
		}
		list := elist.([][]byte)
		if forceDirect {
			list = append(list, value.([]byte))
		} else {
			list = append(list, EncodeToBytes(value))
		}
		this.SyncMap.Store(key, list)
		this.mutex.Unlock()
		return len(list) - 1
	} else {
		encoded2 := this.send(func() []byte {
			return join([][]byte{
				syncMapAppendListCommand,
				[]byte(key),
				EncodeToBytes(value),
			})
		}, forceConnection)
		x := 0
		DecodeFromBytes(encoded2, &x)
		return x
	}
}
func (this *SyncMapServer) AppendList(key string, value interface{}) int {
	return this.appendListImpl(key, value, false, false)
}
func (this *SyncMapServerTransaction) AppendList(key string, value interface{}) int {
	return this.server.appendListImpl(key, value, false, true)
}

// list のサイズを返す
func (this *SyncMapServer) lenOfListImpl(key string, forceDirect, forceConnection bool) int {
	if forceDirect || this.IsOnThisApp() {
		elist, ok := this.SyncMap.Load(key)
		if !ok {
			return 0
		}
		return len(elist.([][]byte))
	} else {
		encoded := this.send(func() []byte {
			return join([][]byte{
				syncMapLenListCommand,
				[]byte(key),
			})
		}, forceConnection)
		x := 0
		DecodeFromBytes(encoded, &x)
		return x
	}
}
func (this *SyncMapServer) LenOfList(key string) int {
	return this.lenOfListImpl(key, false, false)
}
func (this *SyncMapServerTransaction) LenOfList(key string) int {
	return this.server.lenOfListImpl(key, false, true)
}

// NOTE: value はロード可能なようにpointerを渡すこと
func (this *SyncMapServer) loadFromListAtIndexImpl(key string, index int, value interface{}, forceConnection bool) {
	if this.IsOnThisApp() {
		elist, ok := this.SyncMap.Load(key)
		list := elist.([][]byte)
		if !ok || index < 0 || index >= len(list) {
			panic(nil)
		}
		DecodeFromBytes(list[index], value)
	} else {
		encoded := this.send(func() []byte {
			return join([][]byte{
				syncMapIndexListCommand,
				[]byte(key),
				EncodeToBytes(index),
			})
		}, forceConnection)
		DecodeFromBytes(encoded, value)
	}
}
func (this *SyncMapServer) LoadFromListAtIndexImpl(key string, index int, value interface{}) {
	this.loadFromListAtIndexImpl(key, index, value, false)
}
func (this *SyncMapServerTransaction) LoadFromListAtIndexImpl(key string, index int, value interface{}) {
	this.server.loadFromListAtIndexImpl(key, index, value, true)
}

func (this *SyncMapServer) updateListAtIndexImpl(key string, index int, value interface{}, forceDirect, forceConnection bool) {
	if forceDirect || this.IsOnThisApp() {
		elist, ok := this.SyncMap.Load(key)
		if !ok || index < 0 {
			panic(nil)
		}
		list := elist.([][]byte)
		if index >= len(list) {
			panic(nil)
		}
		if forceDirect {
			list[index] = value.([]byte)
		} else {
			list[index] = EncodeToBytes(value)
		}
	} else {
		this.send(func() []byte {
			return join([][]byte{
				syncMapUpdateListCommand,
				[]byte(key),
				EncodeToBytes(index),
				EncodeToBytes(value),
			})
		}, forceConnection)
	}
}
func (this *SyncMapServer) UpdateListAtIndexImpl(key string, index int, value interface{}) {
	this.updateListAtIndexImpl(key, index, value, false, false)
}
func (this *SyncMapServerTransaction) UpdateListAtIndexImpl(key string, index int, value interface{}) {
	this.server.updateListAtIndexImpl(key, index, value, false, true)
}

func (this *SyncMapServer) interpretWrapFunction(buf []byte) []byte {
	ss := split(buf)
	if len(ss) < 1 {
		panic(nil)
	}
	command := ss[0]
	if bytes.Compare(command, syncMapLoadCommand) == 0 {
		key := string(ss[1])
		value, ok := this.SyncMap.Load(key)
		if !ok {
			return []byte("")
		}
		return value.([]byte) // バイト列をそのまま返すので loadImplが使えない
	} else if bytes.Compare(command, syncMapStoreCommand) == 0 {
		key := string(ss[1])
		value := ss[2] // バイト列をそのまま保存するので storeImplが使えない
		this.SyncMap.Store(key, value)
		return []byte("")
	} else if bytes.Compare(command, syncMapLockAllCommand) == 0 {
		this.mutex.Lock()
		return []byte("")
	} else if bytes.Compare(command, syncMapUnlockAllCommand) == 0 {
		this.mutex.Unlock()
		return []byte("")
	} else if bytes.Compare(command, syncMapAddCommand) == 0 {
		value := 0
		DecodeFromBytes(ss[2], &value)
		x := this.addImpl(string(ss[1]), value, true)
		return EncodeToBytes(x)
	} else if bytes.Compare(command, syncMapDeleteCommand) == 0 {
		this.deleteImpl(string(ss[1]), true, false)
		return []byte("")
	} else if bytes.Compare(command, syncMapExistsKeyCommand) == 0 {
		ok := this.exitstsImpl(string(ss[1]), true, false)
		return EncodeToBytes(ok)
	} else if bytes.Compare(command, syncMapInitListCommand) == 0 {
		this.initListImpl(string(ss[1]), true, false)
		return []byte("")
	} else if bytes.Compare(command, syncMapAppendListCommand) == 0 {
		x := this.appendListImpl(string(ss[1]), ss[2], true, false)
		return EncodeToBytes(x)
	} else if bytes.Compare(command, syncMapLenListCommand) == 0 {
		x := this.lenOfListImpl(string(ss[1]), true, false)
		return EncodeToBytes(x)
	} else if bytes.Compare(command, syncMapIndexListCommand) == 0 {
		key := string(ss[1]) // バイト列をそのまま保存するので そのまま使えない
		index := 0
		DecodeFromBytes(ss[2], &index)
		elist, ok := this.SyncMap.Load(key)
		list := elist.([][]byte)
		if !ok || index < 0 || index >= len(list) {
			panic(nil)
		}
		return list[index]
	} else if bytes.Compare(command, syncMapUpdateListCommand) == 0 {
		index := 0
		DecodeFromBytes(ss[2], &index)
		this.updateListAtIndexImpl(string(ss[1]), index, ss[3], true, false)
		return []byte("")
	} else if bytes.Compare(command, syncMapCustomCommand) == 0 {
		return this.sendCustomImpl(func() []byte { return ss[1] }, true, false)
	} else {
		panic(nil)
	}
}
