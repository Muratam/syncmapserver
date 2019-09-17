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

// NOTE: package syncmapserver を main とかに変える
// NOTE: 環境変数 REDIS_HOST に 12.34.56.78 などのIPアドレスを入れる
// NOTE: Redis ラッパーも書くが、ISUCON中はこちらだけ使うことで高速に

const maxSyncMapServerConnectionNum = 15
const RedisHostPrivateIPAddress = "192.168.111.111" // ここで指定したサーバーに
// ` NewSyncMapServer(GetMasterServerAddress()+":8884", MyServerIsOnMasterServerIP()) `
// とすることでSyncMapServerを建てることができる
const SyncMapBackUpPath = "./syncmapbackup-"
const DefaultBackUpTimeSecond = 30 // この秒数毎にバックアップファイルを作成する
func MyServerIsOnMasterServerIP() bool {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return strings.Compare(localAddr.IP.String(), RedisHostPrivateIPAddress) == 0
}
func GetMasterServerAddress() string {
	if MyServerIsOnMasterServerIP() {
		return "127.0.0.1"
	} else {
		return RedisHostPrivateIPAddress
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
type SyncMapServerTransaction struct {
	server      *SyncMapServer
	specificKey bool // 特定のキーのみロックするタイプのやつか
	key         string
}

// NOTE: transaction中にno transactionなものが値を変えてくる可能性が十分にある
//       特に STORE / DELETE はやっかい。だが、たいていこれらはTransactionがついているはずなのでそこまで注意をしなくてもよいのではないか
var syncMapCommandGet = "GET"       // get
var syncMapCommandMGet = "MGET"     // multi get
var syncMapCommandSet = "SET"       // set
var syncMapCommandMSet = "MSET"     // multi set
var syncMapCommandExists = "EXISTS" // check if exists key
var syncMapCommandDel = "DEL"       // delete
var syncMapCommandIncrBy = "INCRBY" // incrBy value
// list (内部的に([]byte ではなく [][]byte として保存しているので) Set / Get は使えない)
// 順序が関係ないものに使うと吉
var syncMapCommandInitList = "INIT_LIST" // init list
var syncMapCommandRPush = "RPUSH"        // append value to list(最初が空でも可能)
var syncMapCommandLLen = "LLEN"          // len of list
var syncMapCommandLIndex = "LINDEX"      // get value from list
var syncMapCommandLSet = "LSET"          // update value at index

// 全てのキーをLockする。
// NOTE: 現在は特定のキーのロックを見ていないのでLockAll()とLockKey()を併用するとデッドロックするかも。
var syncMapCommandLockAll = "LOCK"     // start transaction
var syncMapCommandUnlockAll = "UNLOCK" // end transaction
var syncMapCommandIsLockedAll = "ISLOCKED"

// 特定のキーをLockする。
// それが解除されていれば、 特定のキーをロックする。
var syncMapCommandLockKey = "LOCK_K"     // lock a key
var syncMapCommandUnlockKey = "UNLOCK_K" // unlock a key
var syncMapCommandIsLockedKey = "ISLOCKED_K"

// そのほか
var syncMapCommandKeysCount = "KEYS_COUNT" // key count
var syncMapCommandFlushAll = "FLUSHALL"
var syncMapCommandCustom = "CUSTOM" // custom

// NOTE: LIST_GET_ALL 欲しい？

// bytes utils //////////////////////////////////////////
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
		log.Panic("Too Long Get")
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
	err := gob.NewEncoder(&buf).Encode(x)
	if err != nil {
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
		log.Panic(err)
	}
}

func join(input [][]byte) []byte {
	return EncodeToBytes(input)
}
func split(input []byte) [][]byte {
	result := make([][]byte, 0)
	DecodeFromBytes(input, &result)
	return result
}

// Sync Map Functions ///////////////////////////////////////////
// デフォルトでBackUpが作成される

func NewSyncMapServer(substanceAddress string, isMaster bool) *SyncMapServer {
	if isMaster {
		port, _ := strconv.Atoi(strings.Split(substanceAddress, ":")[1])
		result := newMasterSyncMapServer(port)
		result.MySendCustomFunction = DefaultSendCustomFunction
		return result
	} else {
		result := newSlaveSyncMapServer(substanceAddress)
		result.MySendCustomFunction = DefaultSendCustomFunction
		return result
	}
}

func (this *SyncMapServer) IsMasterServer() bool {
	return len(this.substanceAddress) == 0
}

////////////////////////////////////////////////////////
//            IMPLEMENTATION OF COMMANDS              //
////////////////////////////////////////////////////////

// サーバーで受け取ってコマンドに対応する関数を実行
func (this *SyncMapServer) interpretWrapFunction(buf []byte) []byte {
	ss := split(buf)
	if len(ss) < 1 {
		panic(nil)
	}
	switch string(ss[0]) {
	// General Commands
	case syncMapCommandGet:
		key := string(ss[1])
		value, ok := this.SyncMap.Load(key)
		if ok {
			return value.([]byte)
		}
	case syncMapCommandSet:
		this.storeDirect(string(ss[1]), ss[2])
	case syncMapCommandExists:
		return EncodeToBytes(this.exitstsImpl(string(ss[1]), true, false))
	case syncMapCommandIncrBy:
		value := 0
		DecodeFromBytes(ss[2], &value)
		return EncodeToBytes(this.incrByImpl(string(ss[1]), value, true))
	case syncMapCommandDel:
		this.delImpl(string(ss[1]), true, false)
	case syncMapCommandKeysCount:
		return EncodeToBytes(this.keysCountImpl(true, false))
	// List Command
	case syncMapCommandInitList:
		this.initListImpl(string(ss[1]), true, false)
	case syncMapCommandRPush:
		return EncodeToBytes(this.rpushImpl(string(ss[1]), ss[2], true, false))
	case syncMapCommandLLen:
		return EncodeToBytes(this.llenImpl(string(ss[1]), true, false))
	// Transaction (Lock / Unlock)
	case syncMapCommandLockAll:
		this.LockAll()
	case syncMapCommandUnlockAll:
		this.UnlockAll()
	case syncMapCommandIsLockedAll:
		return EncodeToBytes(this.isLockedAllImpl(true))
	case syncMapCommandLockKey:
		this.LockKey(string(ss[1]))
	case syncMapCommandUnlockKey:
		this.UnlockKey(string(ss[1]))
	case syncMapCommandIsLockedKey:
		return EncodeToBytes(this.isLockedKeyImpl(string(ss[1]), true))
	case syncMapCommandLIndex:
		key := string(ss[1]) // バイト列をそのまま保存するので そのまま使えない
		index := 0
		DecodeFromBytes(ss[2], &index)
		elist, ok := this.SyncMap.Load(key)
		list := elist.([][]byte)
		if !ok || index < 0 || index >= len(list) {
			panic(nil)
		}
		return list[index]
	case syncMapCommandLSet:
		index := 0
		DecodeFromBytes(ss[2], &index)
		this.lsetImpl(string(ss[1]), index, ss[3], true, false)
	// Multi Command (for N+1)
	case syncMapCommandMGet:
		keys := make([]string, 0)
		DecodeFromBytes(ss[1], &keys)
		return join(this.mgetImpl(keys, true, false))
	case syncMapCommandMSet:
		keys := make([]string, 0)
		DecodeFromBytes(ss[1], &keys)
		values := make([][]byte, 0)
		DecodeFromBytes(ss[2], &values)
		this.msetImpl(keys, values, true, false)
	// Custom Command
	case syncMapCommandCustom:
		return this.sendCustomImpl(func() []byte { return ss[1] }, true, false)
	case syncMapCommandFlushAll:
		this.flushAllImpl(true, false)
	default:
		panic(nil)
	}
	return []byte("")
}

// 変更できるようにpointer型で受け取ること。
func (this *SyncMapServer) getImpl(key string, res interface{}, force bool) bool {
	if this.IsMasterServer() {
		return this.loadDirectAsBytes(key, res)
	} else { // やっていき
		loadedBytes := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandGet),
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
func (this *SyncMapServer) Get(key string, res interface{}) bool {
	return this.getImpl(key, res, false)
}
func (this *SyncMapServerTransaction) Get(key string, res interface{}) bool {
	return this.server.getImpl(key, res, true)
}

// Masterなら直に、SlaveならTCPでつないで実行
func (this *SyncMapServer) setImpl(key string, value interface{}, force bool) {
	if this.IsMasterServer() {
		this.storeDirectAsBytes(key, value)
	} else { // やっていき
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandSet),
				[]byte(key),
				EncodeToBytes(value),
			})
		}, force)
	}
}
func (this *SyncMapServer) Set(key string, value interface{}) {
	this.setImpl(key, value, false)
}
func (this *SyncMapServerTransaction) Set(key string, value interface{}) {
	this.server.setImpl(key, value, true)
}

// 変更できるようにpointer型で受け取ること
// Masterなら直に、SlaveならTCPでつないで実行
// あとでDecodeFromBytesすること。空なら 空(非nil)。
func (this *SyncMapServer) mgetImpl(keys []string, forceDirect, forceConnection bool) [][]byte {
	if forceDirect || this.IsMasterServer() {
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
					[]byte(syncMapCommandMGet),
					EncodeToBytes(keys),
				})
			}, forceConnection),
		)
	}
}
func (this *SyncMapServer) MGet(keys []string) [][]byte {
	return this.mgetImpl(keys, false, false)
}
func (this *SyncMapServerTransaction) MGet(keys []string) [][]byte {
	return this.server.mgetImpl(keys, false, true)
}

// EncodeToBytes() した配列を渡すこと。
// Masterなら直に、SlaveならTCPでつないで実行
// あとでDecodeすること。空なら 空。
func (this *SyncMapServer) msetImpl(keys []string, values [][]byte, forceDirect, forceConnection bool) {
	if forceDirect || this.IsMasterServer() {
		for i, key := range keys {
			value := values[i]
			this.storeDirect(key, value)
		}
	} else { // やっていき
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandMSet),
				EncodeToBytes(keys),
				EncodeToBytes(values),
			})
		}, forceConnection)
	}
}
func (this *SyncMapServer) MSet(keys []string, values [][]byte) {
	this.msetImpl(keys, values, false, false)
}
func (this *SyncMapServerTransaction) MSet(keys []string, values [][]byte) {
	this.server.msetImpl(keys, values, false, true)
}

// Masterなら直に、SlaveならTCPでつないで実行
func (this *SyncMapServer) delImpl(key string, forceDirect, forceConnection bool) {
	if forceDirect || this.IsMasterServer() {
		this.deleteDirect(key)
	} else {
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandDel),
				[]byte(key),
			})
		}, forceConnection)
	}
}
func (this *SyncMapServer) Del(key string) {
	this.delImpl(key, false, false)
}
func (this *SyncMapServerTransaction) Del(key string) {
	this.server.delImpl(key, false, true)
}

// 全ての要素を削除する
func (this *SyncMapServer) FlushAllDirect() {
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
func (this *SyncMapServer) flushAllImpl(forceDirect, forceConnection bool) {
	if forceDirect || this.IsMasterServer() {
		this.FlushAllDirect()
	} else {
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandFlushAll),
			})
		}, forceConnection)
	}
}
func (this *SyncMapServer) FlushAll() {
	this.flushAllImpl(false, false)
}
func (this *SyncMapServerTransaction) FlushAll() {
	this.server.flushAllImpl(false, true)
}

// キーが存在するか確認
func (this *SyncMapServer) exitstsImpl(key string, forceDirect, forceConnection bool) bool {
	if forceDirect || this.IsMasterServer() {
		_, ok := this.SyncMap.Load(key)
		return ok
	} else {
		encoded := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandExists),
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

// SyncMapに保存されている要素数を取得
func (this *SyncMapServer) keysCountImpl(forceDirect, forceConnection bool) int {
	if forceDirect || this.IsMasterServer() {
		return this.KeyCount.Get()
	} else {
		encoded := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandKeysCount),
			})
		}, forceConnection)
		result := 0
		DecodeFromBytes(encoded, &result)
		return result
	}
}
func (this *SyncMapServer) KeysCount() int {
	return this.keysCountImpl(false, false)
}
func (this *SyncMapServerTransaction) KeysCount() int {
	return this.server.keysCountImpl(false, true)
}

// += value する
func (this *SyncMapServer) incrByImpl(key string, value int, forceDirect bool) int {
	if forceDirect || this.IsMasterServer() {
		this.LockAll()
		x := 0
		this.loadDirectAsBytes(key, &x)
		x += value
		this.storeDirectAsBytes(key, x)
		this.UnlockAll()
		return x
	} else { // やっていき
		x := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandIncrBy),
				[]byte(key),
				EncodeToBytes(value),
			})
		}, false)
		result := 0
		DecodeFromBytes(x, &result)
		return result
	}
}
func (this *SyncMapServer) IncrBy(key string, value int) int {
	return this.incrByImpl(key, value, false)
}

// トランザクション (キー毎 / 全体)
func (this *SyncMapServer) isLockedAllImpl(forceDirect bool) bool {
	if forceDirect || this.IsMasterServer() {
		return this.IsLocked
	} else { // やっていき
		x := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandIsLockedAll),
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
	if forceDirect || this.IsMasterServer() {
		locked, ok := this.lockedMap.Load(key)
		if !ok {
			return false // NOTE: 存在しない == ロックされていない
		}
		return locked.(bool)
	} else { // やっていき
		x := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandIsLockedKey),
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
	if this.IsMasterServer() {
		this.LockAll()
	} else { // やっていき
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandLockAll),
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
	if this.IsMasterServer() {
		this.LockKey(key)
	} else { // やっていき
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandLockKey),
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
	if this.server.IsMasterServer() {
		if this.specificKey {
			this.server.UnlockKey(this.key)
		} else {
			this.server.UnlockAll()
		}
	} else { // やっていき
		if this.specificKey {
			this.server.send(func() []byte {
				return join([][]byte{
					[]byte(syncMapCommandUnlockKey),
					[]byte(this.key),
				})
			}, true)
		} else {
			this.server.send(func() []byte {
				return join([][]byte{
					[]byte(syncMapCommandUnlockAll),
				})
			}, true)
		}
	}
}

// 配列を初期化する
func (this *SyncMapServer) initListImpl(key string, forceDirect, forceConnection bool) {
	if forceDirect || this.IsMasterServer() {
		// NOTE: 速度が気になれば *[][]byte にすることを検討する
		this.storeDirect(key, make([][]byte, 0))
	} else {
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandInitList),
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

// List に要素を追加したのち index を返す
func (this *SyncMapServer) rpushImpl(key string, value interface{}, forceDirect, forceConnection bool) int {
	if forceDirect || this.IsMasterServer() {
		this.LockKey(key)
		elist, ok := this.SyncMap.Load(key)
		if !ok {
			this.storeDirect(key, make([][]byte, 0))
			elist, _ = this.SyncMap.Load(key)
		}
		list := elist.([][]byte)
		if forceDirect {
			list = append(list, value.([]byte))
		} else {
			list = append(list, EncodeToBytes(value))
		}
		this.storeDirect(key, list)
		this.UnlockKey(key)
		return len(list) - 1
	} else {
		encoded2 := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandRPush),
				[]byte(key),
				EncodeToBytes(value),
			})
		}, forceConnection)
		x := 0
		DecodeFromBytes(encoded2, &x)
		return x
	}
}
func (this *SyncMapServer) RPush(key string, value interface{}) int {
	return this.rpushImpl(key, value, false, false)
}
func (this *SyncMapServerTransaction) RPush(key string, value interface{}) int {
	return this.server.rpushImpl(key, value, false, true)
}

// list のサイズを返す
func (this *SyncMapServer) llenImpl(key string, forceDirect, forceConnection bool) int {
	if forceDirect || this.IsMasterServer() {
		elist, ok := this.SyncMap.Load(key)
		if !ok {
			return 0
		}
		return len(elist.([][]byte))
	} else {
		encoded := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandLLen),
				[]byte(key),
			})
		}, forceConnection)
		x := 0
		DecodeFromBytes(encoded, &x)
		return x
	}
}
func (this *SyncMapServer) LLen(key string) int {
	return this.llenImpl(key, false, false)
}
func (this *SyncMapServerTransaction) LLen(key string) int {
	return this.server.llenImpl(key, false, true)
}

// List を　Get する / value はロード可能なようにpointerを渡すこと
func (this *SyncMapServer) lindexImpl(key string, index int, value interface{}, forceConnection bool) bool {
	if this.IsMasterServer() {
		elist, ok := this.SyncMap.Load(key)
		list := elist.([][]byte)
		if !ok || index < 0 || index >= len(list) {
			return false
		}
		DecodeFromBytes(list[index], value)
		return true
	} else {
		encoded := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandLIndex),
				[]byte(key),
				EncodeToBytes(index),
			})
		}, forceConnection)
		if len(encoded) == 0 {
			return false
		}
		DecodeFromBytes(encoded, value)
		return true
	}
}
func (this *SyncMapServer) LIndex(key string, index int, value interface{}) bool {
	return this.lindexImpl(key, index, value, false)
}
func (this *SyncMapServerTransaction) LIndex(key string, index int, value interface{}) bool {
	return this.server.lindexImpl(key, index, value, true)
}

// List を Update する
func (this *SyncMapServer) lsetImpl(key string, index int, value interface{}, forceDirect, forceConnection bool) {
	if forceDirect || this.IsMasterServer() {
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
				[]byte(syncMapCommandLSet),
				[]byte(key),
				EncodeToBytes(index),
				EncodeToBytes(value),
			})
		}, forceConnection)
	}
}
func (this *SyncMapServer) LSet(key string, index int, value interface{}) {
	this.lsetImpl(key, index, value, false, false)
}
func (this *SyncMapServerTransaction) LSet(key string, index int, value interface{}) {
	this.server.lsetImpl(key, index, value, false, true)
}

// 自作関数を使用する時用
func DefaultSendCustomFunction(this *SyncMapServer, buf []byte) []byte {
	return buf // echo server
}
func (this *SyncMapServer) sendCustomImpl(f func() []byte, forceDirect, forceConnect bool) []byte {
	if forceDirect || this.IsMasterServer() {
		return this.MySendCustomFunction(this, f())
	} else {
		return this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandCustom),
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

//  SyncMap で使用する関数

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
	this.readFile()
	// バックアッププロセスを開始する
	this.startBackUpProcess()
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
func (this *SyncMapServer) getPath() string {
	return SyncMapBackUpPath + strconv.Itoa(this.masterPort) + ".sm"
}
func (this *SyncMapServer) writeFile() {
	if !this.IsMasterServer() {
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
	file, err := os.Create(this.getPath())
	if err != nil {
		panic(err)
	}
	defer file.Close()
	file.Write(EncodeToBytes(result))
}
func (this *SyncMapServer) readFile() {
	if !this.IsMasterServer() {
		return
	}
	// Lock ?
	encoded, err := ioutil.ReadFile(this.getPath())
	if err != nil {
		// fmt.Println("no " + this.getPath() + "exists.")
		return
	}
	this.FlushAllDirect()
	decoded := make([][][]byte, 0)
	DecodeFromBytes(encoded, &decoded)
	for _, here := range decoded {
		key := string(here[0])
		t := string(here[1])
		if strings.Compare(t, "1") == 0 {
			this.storeDirect(key, here[2])
		} else if strings.Compare(t, "2") == 0 {
			this.storeDirect(key, here[2:])
		} else {
			panic(nil)
		}
	}
}
func (this *SyncMapServer) startBackUpProcess() {
	go func() {
		time.Sleep(time.Duration(DefaultBackUpTimeSecond) * time.Second)
		this.writeFile()
	}()
}

// 自身の SyncMapからLoad / 変更できるようにpointer型で受け取ること
func (this *SyncMapServer) loadDirectAsBytes(key string, res interface{}) bool {
	value, ok := this.SyncMap.Load(key)
	if ok {
		DecodeFromBytes(value.([]byte), res)
	}
	return ok
}

// 自身の SyncMapにStore
func (this *SyncMapServer) storeDirect(key string, value interface{}) {
	_, exists := this.SyncMap.Load(key)
	if !exists {
		var m sync.Mutex
		this.mutexMap.Store(key, &m)
		this.lockedMap.Store(key, false)
		this.KeyCount.Inc()
	}
	this.SyncMap.Store(key, value)
}
func (this *SyncMapServer) deleteDirect(key string) {
	_, exists := this.SyncMap.Load(key)
	if !exists {
		return
	}
	this.SyncMap.Delete(key)
	this.mutexMap.Delete(key)
	this.lockedMap.Delete(key)
	this.KeyCount.Dec()
}
func (this *SyncMapServer) storeDirectAsBytes(key string, value interface{}) {
	encoded := EncodeToBytes(value)
	this.storeDirect(key, encoded)
}

// 生のbyteを送信
func (this *SyncMapServer) send(f func() []byte, force bool) []byte {
	if this.IsMasterServer() {
		return this.interpretWrapFunction(f())
	} else {
		return this.sendBySlave(f, force)
	}
}
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

// MutexInt ////////////////////////////////////////////
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
