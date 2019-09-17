package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// NOTE: 環境変数 REDIS_HOST に 12.34.56.78 などのIPアドレスを入れる

const maxSyncMapServerConnectionNum = 15
const MasterServerAddressWhenNO_REDIS_HOST = "12.34.56.78"
const SyncMapBackUpPath = "./syncmapbackup-"
const DefaultBackUpTimeSecond = 30 // この秒数毎にバックアップファイルを作成する
func IsMasterServerIP() bool {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return strings.Compare(localAddr.IP.String(), parseRedisHostIP()) == 0
}
func GetMasterServerAddress() string {
	if IsMasterServerIP() {
		return "127.0.0.1"
	} else {
		return parseRedisHostIP()
	}
}
func parseRedisHostIP() string {
	result := os.Getenv("REDIS_HOST")
	if result == "" {
		return MasterServerAddressWhenNO_REDIS_HOST
	}
	return result
}

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
// 20000Byteを超える際の Linux上での調子が悪いので、
// 先にContent-Lengthを32bit(4Byte)指定して読み込ませている
func readAll(conn net.Conn) []byte {
	contentLen := 0
	if true {
		buf := make([]byte, 4)
		readBufNum, err := conn.Read(buf)
		if readBufNum != 4 {
			if readBufNum == 0 {
				// WARN
				return readAll(conn)
			} else {
				// WARN
				log.Panic("too short buf : ", readBufNum)
			}
		}
		contentLen += int(buf[0])
		contentLen += int(buf[1]) << 8
		contentLen += int(buf[2]) << 16
		contentLen += int(buf[3]) << 24
		if contentLen == 0 {
			return []byte("")
		}
		if err != nil {
			log.Panic(err)
		}
	}
	readMax := 1024
	bufAll := make([]byte, 0)
	currentReadLen := 0
	for currentReadLen < contentLen {
		buf := make([]byte, readMax)
		readBufNum, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				if currentReadLen+readBufNum != contentLen {
					log.Panic("invalid len TCP")
				}
				return append(bufAll, buf[:readBufNum]...)
			} else {
				log.Panic(err)
			}
		}
		if readBufNum == 0 {
			continue
		}
		currentReadLen += readBufNum
		bufAll = append(bufAll, buf[:readBufNum]...)
	}
	if currentReadLen > contentLen {
		log.Panic("Too Long Load")
	}
	return bufAll
}
func writeAll(conn net.Conn, content []byte) {
	contentLen := len(content)
	if contentLen >= 4294967296 {
		log.Panic("Too Long Content", contentLen)
	}
	conn.Write([]byte{
		byte((contentLen & 0x000000ff) >> 0),
		byte((contentLen & 0x0000ff00) >> 8),
		byte((contentLen & 0x00ff0000) >> 16),
		byte((contentLen & 0xff000000) >> 24),
	})
	conn.Write(content)
}

func EncodeToBytes(x interface{}) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(x); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// 変更できるようにpointer型で受け取ること
func DecodeFromBytes(bytes_ []byte, x interface{}) {
	var buf bytes.Buffer
	buf.Write(bytes_)
	dec := gob.NewDecoder(&buf)
	err := dec.Decode(x)
	if err != nil {
		log.Println("PARSE ERRR|OR:")
		log.Println(len(bytes_), ":", bytes_)
		log.Println(x)
		log.Panic(err)
	}
}

// SyncMapServer
type SyncMapServer struct {
	SyncMap              sync.Map // string -> (byte[] | byte[][])
	KeyCount             MutexInt
	substanceAddress     string
	masterPort           int
	connectNum           MutexInt
	mutex                sync.Mutex
	IsLocked             bool
	mutexMap             sync.Map // string -> sync.Mutex
	lockedMap            sync.Map // string -> bool
	MySendCustomFunction func(this *SyncMapServer, buf []byte) []byte
}

func DefaultSendCustomFunction(this *SyncMapServer, buf []byte) []byte {
	return buf // echo server
}
func (this *SyncMapServer) GetPath() string {
	return SyncMapBackUpPath + strconv.Itoa(this.masterPort) + ".sm"
}
func (this *SyncMapServer) WriteFile() {
	if !this.IsOnThisApp() {
		return
	}
	// Lock が必要？
	result := make([][][]byte, 0)
	this.SyncMap.Range(func(key, value interface{}) bool {
		here := make([][]byte, 0)
		here = append(here, []byte(key.(string)))
		if bs, ok := value.([]byte); ok {
			here = append(here, []byte("1"))
			here = append(here, bs)
		} else if bss, ok := value.([][]byte); ok {
			here = append(here, []byte("2"))
			here = append(here, bss...)
		} else {
			panic(nil)
		}
		result = append(result, here)
		return true
	})
	file, err := os.Create(this.GetPath())
	if err != nil {
		panic(err)
	}
	defer file.Close()
	file.Write(EncodeToBytes(result))
}
func (this *SyncMapServer) ReadFile() {
	if !this.IsOnThisApp() {
		return
	}
	// Lock ?
	encoded, err := ioutil.ReadFile(this.GetPath())
	if err != nil {
		fmt.Println("no " + this.GetPath() + "exists.")
		return
	}
	this.ClearAllDirect()
	decoded := make([][][]byte, 0)
	DecodeFromBytes(encoded, &decoded)
	for _, here := range decoded {
		key := string(here[0])
		t := string(here[1])
		if strings.Compare(t, "1") == 0 {
			this.StoreDirect(key, here[2])
		} else if strings.Compare(t, "2") == 0 {
			this.StoreDirect(key, here[2:])
		} else {
			panic(nil)
		}
	}
}
func (this *SyncMapServer) StartBackUpProcess() {
	go func() {
		time.Sleep(time.Duration(DefaultBackUpTimeSecond) * time.Second)
		this.WriteFile()
	}()
}

func newMasterSyncMapServer(port int) *SyncMapServer {
	this := &SyncMapServer{}
	this.substanceAddress = ""
	this.masterPort = port
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
				writeAll(conn, this.interpretWrapFunction(readAll(conn)))
				conn.Close()
			}()
		}
	}()
	// 起動終了までちょっと時間がかかるかもしれないので待機しておく
	time.Sleep(10 * time.Millisecond)
	// 何も設定しなければecho
	this.MySendCustomFunction = func(this *SyncMapServer, buf []byte) []byte { return buf }
	// バックアップファイルが見つかればそれを読み込む
	this.ReadFile()
	// バックアッププロセスを開始する
	this.StartBackUpProcess()
	return this
}
func newSlaveSyncMapServer(substanceAddress string) *SyncMapServer {
	this := &SyncMapServer{}
	this.substanceAddress = substanceAddress
	port, err := strconv.Atoi(strings.Split(substanceAddress, ":")[1])
	if err != nil {
		panic(err)
	}
	this.masterPort = port
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
	writeAll(conn, f())
	result := readAll(conn)
	conn.Close()
	this.connectNum.Dec()
	return result
}

// public methods
func (this *SyncMapServer) IsOnThisApp() bool {
	return len(this.substanceAddress) == 0
}

// 自身の SyncMapからLoad
// 変更できるようにpointer型で受け取ること
func (this *SyncMapServer) LoadDirectAsBytes(key string, res interface{}) bool {
	value, ok := this.SyncMap.Load(key)
	if ok {
		DecodeFromBytes(value.([]byte), res)
	}
	return ok
}

// 自身の SyncMapにStore
func (this *SyncMapServer) StoreDirect(key string, value interface{}) {
	_, exists := this.SyncMap.Load(key)
	if !exists {
		var m sync.Mutex
		this.mutexMap.Store(key, &m)
		this.lockedMap.Store(key, false)
		this.KeyCount.Inc()
	}
	this.SyncMap.Store(key, value)
}
func (this *SyncMapServer) DeleteDirect(key string) {
	_, exists := this.SyncMap.Load(key)
	if !exists {
		return
	}
	this.SyncMap.Delete(key)
	this.mutexMap.Delete(key)
	this.lockedMap.Delete(key)
	this.KeyCount.Dec()
}
func (this *SyncMapServer) StoreDirectAsBytes(key string, value interface{}) {
	encoded := EncodeToBytes(value)
	this.StoreDirect(key, encoded)
}

// NOTE: transaction中にno transactionなものが値を変えてくる可能性が十分にある
//       特に STORE / DELETE はやっかい。だが、たいていこれらはTransactionがついているはずなのでそこまで注意をしなくてもよいのではないか
var syncMapCustomCommand = "CT"        // custom
var syncMapLoadCommand = "GET"         // load
var syncMapMultiLoadCommand = "MGET"   // multi load
var syncMapStoreCommand = "SET"        // store
var syncMapMultiStoreCommand = "MSET"  // multi set
var syncMapAddCommand = "ADD"          // add value
var syncMapExistsKeyCommand = "EXISTS" // check if exists key
var syncMapDeleteCommand = "DEL"       // delete
// list (内部的に([]byte ではなく [][]byte として保存しているので) Store / Load は使えない)
// 順序が関係ないものに使うと吉
var syncMapInitListCommand = "INIT_LIST"      // init list
var syncMapAppendListCommand = "APPEND_LIST"  // append value to list(最初が空でも可能)
var syncMapLenListCommand = "LEN_LIST"        // len of list
var syncMapIndexListCommand = "INDEX_LIST"    // get value from list
var syncMapUpdateListCommand = "UPDATE_LIST"  // update value at index
var syncMapGetAllListCommand = "GET_ALL_LIST" // list の要素を全て取得

// 全てのキーをLockする。
// NOTE: 現在は特定のキーのロックを見ていないのでLockAll()とLockKey()を併用するとデッドロックするかも。
var syncMapLockAllCommand = "LOCK"     // start transaction
var syncMapUnlockAllCommand = "UNLOCK" // end transaction
var syncMapIsLockedAllCommand = "ISLOCKED"

// 特定のキーをLockする。
// それが解除されていれば、 特定のキーをロックする。
var syncMapLockKeyCommand = "LOCK_K"     // lock a key
var syncMapUnlockKeyCommand = "UNLOCK_K" // unlock a key
var syncMapIsLockedKeyCommand = "ISLOCKED_K"
var syncMapLengthCommand = "LEN" // key count
var syncMapClearAllCommand = "CLEAR_ALL"

// LIST_GET_ALL 欲しい？

type SyncMapServerTransaction struct {
	server      *SyncMapServer
	specificKey bool // 特定のキーのみロックするタイプのやつか
	key         string
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
				[]byte(syncMapCustomCommand),
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

// 変更できるようにpointer型で受け取ること
// Masterなら直に、SlaveならTCPでつないで実行
func (this *SyncMapServer) loadImpl(key string, res interface{}, force bool) bool {
	if this.IsOnThisApp() {
		return this.LoadDirectAsBytes(key, res)
	} else { // やっていき
		loadedBytes := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapLoadCommand),
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

// 変更できるようにpointer型で受け取ること
// Masterなら直に、SlaveならTCPでつないで実行
// あとでDecodeFromBytesすること。空なら 空(非nil)。
func (this *SyncMapServer) multiLoadImpl(keys []string, forceDirect, forceConnection bool) [][]byte {
	if forceDirect || this.IsOnThisApp() {
		result := make([][]byte, 0)
		for _, key := range keys {
			value, ok := this.SyncMap.Load(key)
			if !ok {
				result = append(result, make([]byte, 0))
			} else {
				result = append(result, value.([]byte))
			}
		}
		return result
	} else { // やっていき
		return split(
			this.send(func() []byte {
				return join([][]byte{
					[]byte(syncMapMultiLoadCommand),
					EncodeToBytes(keys),
				})
			}, forceConnection),
		)
	}
}
func (this *SyncMapServer) MultiLoad(keys []string) [][]byte {
	return this.multiLoadImpl(keys, false, false)
}
func (this *SyncMapServerTransaction) MultiLoad(keys []string) [][]byte {
	return this.server.multiLoadImpl(keys, false, true)
}

// Masterなら直に、SlaveならTCPでつないで実行
func (this *SyncMapServer) storeImpl(key string, value interface{}, force bool) {
	if this.IsOnThisApp() {
		this.StoreDirectAsBytes(key, value)
	} else { // やっていき
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapStoreCommand),
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

// EncodeToBytes() した配列を渡すこと。
// Masterなら直に、SlaveならTCPでつないで実行
// あとでDecodeすること。空なら 空。
func (this *SyncMapServer) multiStoreImpl(keys []string, values [][]byte, forceDirect, forceConnection bool) {
	if forceDirect || this.IsOnThisApp() {
		for i, key := range keys {
			value := values[i]
			this.StoreDirect(key, value)
		}
	} else { // やっていき
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapMultiStoreCommand),
				EncodeToBytes(keys),
				EncodeToBytes(values),
			})
		}, forceConnection)
	}
}
func (this *SyncMapServer) MultiStore(keys []string, values [][]byte) {
	this.multiStoreImpl(keys, values, false, false)
}
func (this *SyncMapServerTransaction) MultiStore(keys []string, values [][]byte) {
	this.server.multiStoreImpl(keys, values, false, true)
}

// Masterなら直に、SlaveならTCPでつないで実行
func (this *SyncMapServer) deleteImpl(key string, forceDirect, forceConnection bool) {
	if forceDirect || this.IsOnThisApp() {
		this.DeleteDirect(key)
	} else {
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapDeleteCommand),
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

func (this *SyncMapServer) ClearAllDirect() {
	var syncMap sync.Map
	this.SyncMap = syncMap
	var mutexMap sync.Map
	this.mutexMap = mutexMap
	var lockedMap sync.Map
	this.lockedMap = lockedMap
	var keyCount MutexInt
	this.KeyCount = keyCount
	// WARN: connectNum
	var mutex sync.Mutex
	this.mutex = mutex
	this.IsLocked = false
}
func (this *SyncMapServer) clearAllImpl(forceDirect, forceConnection bool) {
	if forceDirect || this.IsOnThisApp() {
		this.ClearAllDirect()
	} else {
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapClearAllCommand),
			})
		}, forceConnection)
	}
}
func (this *SyncMapServer) ClearAll() {
	this.clearAllImpl(false, false)
}
func (this *SyncMapServerTransaction) ClearAll() {
	this.server.clearAllImpl(false, true)
}

// Masterなら直に、SlaveならTCPでつないで実行
func (this *SyncMapServer) exitstsImpl(key string, forceDirect, forceConnection bool) bool {
	if forceDirect || this.IsOnThisApp() {
		_, ok := this.SyncMap.Load(key)
		return ok
	} else {
		encoded := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapExistsKeyCommand),
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
func (this *SyncMapServer) getLenImpl(forceDirect, forceConnection bool) int {
	if forceDirect || this.IsOnThisApp() {
		return this.KeyCount.Get()
	} else {
		encoded := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapLengthCommand),
			})
		}, forceConnection)
		result := 0
		DecodeFromBytes(encoded, &result)
		return result
	}
}
func (this *SyncMapServer) GetLen() int {
	return this.getLenImpl(false, false)
}
func (this *SyncMapServerTransaction) GetLen() int {
	return this.server.getLenImpl(false, true)
}

// Masterなら直に、SlaveならTCPでつないで実行
func (this *SyncMapServer) addImpl(key string, value int, forceDirect bool) int {
	if forceDirect || this.IsOnThisApp() {
		this.LockAll()
		x := 0
		this.LoadDirectAsBytes(key, &x)
		x += value
		this.StoreDirectAsBytes(key, x)
		this.UnlockAll()
		return x
	} else { // やっていき
		x := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapAddCommand),
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
func (this *SyncMapServer) isLockedAllImpl(forceDirect bool) bool {
	if forceDirect || this.IsOnThisApp() {
		return this.IsLocked
	} else { // やっていき
		x := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapIsLockedAllCommand),
			})
		}, false)
		result := true
		DecodeFromBytes(x, &result)
		return result
	}
}
func (this *SyncMapServer) IsLockedAll() bool {
	return this.isLockedAllImpl(false)
}
func (this *SyncMapServer) isLockedKeyImpl(key string, forceDirect bool) bool {
	if forceDirect || this.IsOnThisApp() {
		locked, ok := this.lockedMap.Load(key)
		if !ok {
			return false // NOTE: 存在しない == ロックされていない
		}
		return locked.(bool)
	} else { // やっていき
		x := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapIsLockedKeyCommand),
				[]byte(key),
			})
		}, false)
		result := true
		DecodeFromBytes(x, &result)
		return result
	}
}
func (this *SyncMapServer) IsLockedKey(key string) bool {
	return this.isLockedKeyImpl(key, false)
}

func (this *SyncMapServer) LockAll() {
	this.mutex.Lock()
	this.IsLocked = true
}
func (this *SyncMapServer) UnlockAll() {
	this.IsLocked = false
	this.mutex.Unlock()
}
func (this *SyncMapServer) LockKey(key string) {
	m, ok := this.mutexMap.Load(key)
	if !ok {
		panic(nil)
	}
	m.(*sync.Mutex).Lock()
	this.lockedMap.Store(key, true)
}
func (this *SyncMapServer) UnlockKey(key string) {
	m, ok := this.mutexMap.Load(key)
	if !ok {
		panic(nil)
	}
	this.lockedMap.Store(key, false)
	m.(*sync.Mutex).Unlock()
}

func (this *SyncMapServer) StartTransaction(f func(this *SyncMapServerTransaction)) {
	if this.IsOnThisApp() {
		this.LockAll()
	} else { // やっていき
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapLockAllCommand),
			})
		}, false)
	}
	var tx SyncMapServerTransaction
	tx.server = this
	tx.specificKey = false
	tx.key = ""
	f(&tx)
	tx.endTransaction()
}
func (this *SyncMapServer) StartTransactionWithKey(key string, f func(this *SyncMapServerTransaction)) {
	if this.IsOnThisApp() {
		this.LockKey(key)
	} else { // やっていき
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapLockKeyCommand),
				[]byte(key),
			})
		}, false)
	}
	var tx SyncMapServerTransaction
	tx.server = this
	tx.specificKey = true
	tx.key = key
	f(&tx)
	tx.endTransaction()
}

func (this *SyncMapServerTransaction) endTransaction() {
	if this.server.IsOnThisApp() {
		if this.specificKey {
			this.server.UnlockKey(this.key)
		} else {
			this.server.UnlockAll()
		}
	} else { // やっていき
		if this.specificKey {
			this.server.send(func() []byte {
				return join([][]byte{
					[]byte(syncMapUnlockKeyCommand),
					[]byte(this.key),
				})
			}, true)
		} else {
			this.server.send(func() []byte {
				return join([][]byte{
					[]byte(syncMapUnlockAllCommand),
				})
			}, true)
		}
	}
}

// 配列を初期化する
func (this *SyncMapServer) initListImpl(key string, forceDirect, forceConnection bool) {
	if forceDirect || this.IsOnThisApp() {
		// NOTE: 速度が気になれば *[][]byte にすることを検討する
		this.StoreDirect(key, make([][]byte, 0))
	} else {
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapInitListCommand),
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
		this.LockKey(key)
		elist, ok := this.SyncMap.Load(key)
		if !ok {
			this.StoreDirect(key, make([][]byte, 0))
			elist, _ = this.SyncMap.Load(key)
		}
		list := elist.([][]byte)
		if forceDirect {
			list = append(list, value.([]byte))
		} else {
			list = append(list, EncodeToBytes(value))
		}
		this.StoreDirect(key, list)
		this.UnlockKey(key)
		return len(list) - 1
	} else {
		encoded2 := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapAppendListCommand),
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
				[]byte(syncMapLenListCommand),
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

// value はロード可能なようにpointerを渡すこと
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
				[]byte(syncMapIndexListCommand),
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
				[]byte(syncMapUpdateListCommand),
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
	switch string(ss[0]) {
	// General Commands
	case syncMapLoadCommand:
		key := string(ss[1])
		value, ok := this.SyncMap.Load(key)
		if ok {
			return value.([]byte)
		}
	case syncMapStoreCommand:
		this.StoreDirect(string(ss[1]), ss[2])
	case syncMapExistsKeyCommand:
		return EncodeToBytes(this.exitstsImpl(string(ss[1]), true, false))
	case syncMapAddCommand:
		value := 0
		DecodeFromBytes(ss[2], &value)
		return EncodeToBytes(this.addImpl(string(ss[1]), value, true))
	case syncMapDeleteCommand:
		this.deleteImpl(string(ss[1]), true, false)
	case syncMapLengthCommand:
		return EncodeToBytes(this.getLenImpl(true, false))
	// List Command
	case syncMapInitListCommand:
		this.initListImpl(string(ss[1]), true, false)
	case syncMapAppendListCommand:
		return EncodeToBytes(this.appendListImpl(string(ss[1]), ss[2], true, false))
	case syncMapLenListCommand:
		return EncodeToBytes(this.lenOfListImpl(string(ss[1]), true, false))
	// Transaction (Lock / Unlock)
	case syncMapLockAllCommand:
		this.LockAll()
	case syncMapUnlockAllCommand:
		this.UnlockAll()
	case syncMapIsLockedAllCommand:
		return EncodeToBytes(this.isLockedAllImpl(true))
	case syncMapLockKeyCommand:
		this.LockKey(string(ss[1]))
	case syncMapUnlockKeyCommand:
		this.UnlockKey(string(ss[1]))
	case syncMapIsLockedKeyCommand:
		return EncodeToBytes(this.isLockedKeyImpl(string(ss[1]), true))
	case syncMapIndexListCommand:
		key := string(ss[1]) // バイト列をそのまま保存するので そのまま使えない
		index := 0
		DecodeFromBytes(ss[2], &index)
		elist, ok := this.SyncMap.Load(key)
		list := elist.([][]byte)
		if !ok || index < 0 || index >= len(list) {
			panic(nil)
		}
		return list[index]
	case syncMapUpdateListCommand:
		index := 0
		DecodeFromBytes(ss[2], &index)
		this.updateListAtIndexImpl(string(ss[1]), index, ss[3], true, false)
	// Multi Command (for N+1)
	case syncMapMultiLoadCommand:
		keys := make([]string, 0)
		DecodeFromBytes(ss[1], &keys)
		return join(this.multiLoadImpl(keys, true, false))
	case syncMapMultiStoreCommand:
		keys := make([]string, 0)
		DecodeFromBytes(ss[1], &keys)
		values := make([][]byte, 0)
		DecodeFromBytes(ss[2], &values)
		this.multiStoreImpl(keys, values, true, false)
	// Custom Command
	case syncMapCustomCommand:
		return this.sendCustomImpl(func() []byte { return ss[1] }, true, false)
	case syncMapClearAllCommand:
		this.clearAllImpl(true, false)
	default:
		panic(nil)
	}
	return []byte("")
}
