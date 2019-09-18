package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-collections/collections/stack"
)

// NOTE: package syncmapserver を main とかに変える
// NOTE: 環境変数 REDIS_HOST に 12.34.56.78 などのIPアドレスを入れる
// NOTE: Redis ラッパーも書くが、ISUCON中はこちらだけ使うことで高速に
const maxSyncMapServerConnectionNum = 16
const defaultReadBufferSize = 8192                  // ガッと取ったほうが良い。メモリを使用したくなければ 1024.逆なら65536
const RedisHostPrivateIPAddress = "192.168.111.111" // ここで指定したサーバーに
// ` NewSyncMapServer(GetMasterServerAddress()+":8884", MyServerIsOnMasterServerIP()) `
// とすることでSyncMapServerを建てることができる
const SyncMapBackUpPath = "./syncmapbackup-" // カレントディレクトリにバックアップを作成。パーミッションに注意。
const DefaultBackUpTimeSecond = 30           // この秒数毎にバックアップファイルを作成する

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
	SyncMap   sync.Map // string -> (byte[] | byte[][])
	mutexMap  sync.Map // string -> sync.Mutex
	lockedMap sync.Map // string -> bool
	KeyCount  int32
	// 接続情報
	substanceAddress string
	masterPort       int
	// コネクションはプールして再利用する
	connectionPool                     []net.Conn
	connectionPoolStatus               []int
	connectionPoolEmptyIndexStack      *stack.Stack
	connectionPoolEmptyIndexStackMutex sync.Mutex
	// 関数をカスタマイズする用
	MySendCustomFunction func(this *SyncMapServer, buf []byte) []byte
	// 全体としてのロック用
	mutex    sync.Mutex
	IsLocked bool
}

const (
	ConnectionPoolStatusDisconnected = iota // = 0 未接続
	ConnectionPoolStatusUsing
	ConnectionPoolStatusEmpty
)

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
var syncMapCommandDBSize = "DBSIZE" // stored key count
// list (内部的に([]byte ではなく [][]byte として保存しているので) Set / Get は使えない)
// 順序が関係ないものに使うと吉
var syncMapCommandRPush = "RPUSH"   // append value to list(最初が空でも可能)
var syncMapCommandLLen = "LLEN"     // len of list
var syncMapCommandLIndex = "LINDEX" // get value from list
var syncMapCommandLSet = "LSET"     // update value at index

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
var syncMapCommandFlushAll = "FLUSHALL"
var syncMapCommandCustom = "CUSTOM" // custom

// NOTE: LIST_GET_ALL 欲しい？

// bytes utils //////////////////////////////////////////
// 20000Byteを超える際の Linux上での調子が悪いので、
// 先にContent-Lengthを32bit(4Byte)指定して読み込ませている
func parse32bit(input []byte) int {
	result := 0
	result += int(input[0])
	result += int(input[1]) << 8
	result += int(input[2]) << 16
	result += int(input[3]) << 24
	return result
}
func format32bit(input int) []byte {
	return []byte{
		byte(input & 0x000000ff),
		byte((input & 0x0000ff00) >> 8),
		byte((input & 0x00ff0000) >> 16),
		byte((input & 0xff000000) >> 24),
	}
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func readAll(conn net.Conn) []byte {
	contentLen := 0
	if true {
		buf := make([]byte, 4)
		readBufNum, err := conn.Read(buf)
		if readBufNum != 4 {
			if readBufNum == 0 {
				// WARN:
				return readAll(conn)
			} else {
				// WARN
				log.Panic("too short buf : ", readBufNum)
			}
		}
		contentLen += parse32bit(buf)
		if contentLen == 0 {
			return []byte("")
		}
		if err != nil {
			log.Panic(err)
		}
	}
	readMax := defaultReadBufferSize
	var bufAll []byte
	currentReadLen := 0
	for currentReadLen < contentLen {
		readLen := min(readMax, contentLen-currentReadLen)
		buf := make([]byte, readLen)
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
	conn.Write(format32bit(contentLen))
	conn.Write(content)
}

// []byte
// <-> interface{} :: encodeToBytes  <-> decodeFromBytes
// <-> [][]byte    :: join           <-> split
// <-> []string    :: joinStrsToBytes <-> splitBytesToStrs
// <-> string      :: byte[]()       <-> string()
// type MarshalUnmarshalForSpeedUp interface {
// 	MarshalB(buf []byte) ([]byte, error)
// 	UnmarshalB(buf []byte) (uint64, error)
// }
func MarshalString(this string) []byte {
	return []byte(this)
}
func UnmarshalString(this *string, x []byte) {
	(*this) = string(x)
}
func encodeToBytes(x interface{}) []byte {
	// if p, ok := x.(User); ok {
	// 	byf, _ := p.Marshal([]byte{})
	// 	return byf
	// }
	if p, ok := x.(string); ok {
		return MarshalString(p)
	}
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(x)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// 変更できるようにpointer型で受け取ること
func decodeFromBytes(input []byte, x interface{}) {
	// if p, ok := x.(*User); ok {
	// 	(*p).Unmarshal(input)
	// 	return
	// }
	if p, ok := x.(*string); ok {
		UnmarshalString(p, input)
		return
	}
	var buf bytes.Buffer
	buf.Write(input)
	dec := gob.NewDecoder(&buf)
	err := dec.Decode(x)
	if err != nil {
		log.Panic(err)
	}
}

func join(input [][]byte) []byte {
	// 要素数 4B (32bit)
	// (各長さ 4B + データ)を 要素数回
	totalSize := 4 + len(input)*4
	num := len(input)
	for i := 0; i < num; i++ {
		totalSize += len(input[i])
	}
	result := make([]byte, totalSize)
	copy(result[0:4], format32bit(num))
	now := 4
	for _, bs := range input {
		bsLen := len(bs)
		copy(result[now:4+now], format32bit(bsLen))
		now += 4
		copy(result[now:now+bsLen], bs[:])
		now += bsLen
	}
	return result
}
func split(input []byte) [][]byte {
	num := parse32bit(input[:4])
	now := 4
	result := make([][]byte, num)
	for i := 0; i < num; i++ {
		bsLen := parse32bit(input[now : now+4])
		now += 4
		result[i] = input[now : now+bsLen]
		now += bsLen
	}
	return result
}
func joinStrsToBytes(input []string) []byte {
	totalSize := 4 + len(input)*4
	num := len(input)
	for i := 0; i < num; i++ {
		totalSize += len(input[i])
	}
	result := make([]byte, totalSize)
	copy(result[0:4], format32bit(num))
	now := 4
	for _, bs := range input {
		bsLen := len(bs)
		copy(result[now:4+now], format32bit(bsLen))
		now += 4
		copy(result[now:now+bsLen], ([]byte(bs))[:])
		now += bsLen
	}
	return result
}
func splitBytesToStrs(input []byte) []string {
	num := parse32bit(input[:4])
	now := 4
	result := make([]string, num)
	for i := 0; i < num; i++ {
		bsLen := parse32bit(input[now : now+4])
		now += 4
		result[i] = string(input[now : now+bsLen])
		now += bsLen
	}
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
	// fmt.Println("RECIEVED:", len(buf), "Bytes")
	switch string(ss[0]) {
	// General Commands
	case syncMapCommandGet:
		key := string(ss[1])
		value, ok := this.loadDirect(key)
		if ok {
			return value.([]byte)
		}
	case syncMapCommandSet:
		this.storeDirect(string(ss[1]), ss[2])
	case syncMapCommandExists:
		return encodeToBytes(this.exitstsImpl(string(ss[1]), true, false))
	case syncMapCommandIncrBy:
		value := 0
		decodeFromBytes(ss[2], &value)
		return encodeToBytes(this.incrByImpl(string(ss[1]), value, true))
	case syncMapCommandDel:
		this.delImpl(string(ss[1]), true, false)
	case syncMapCommandDBSize:
		return encodeToBytes(this.dbSizeImpl(true, false))
	// List Command
	case syncMapCommandRPush:
		return encodeToBytes(this.rpushImpl(string(ss[1]), ss[2], true, false))
	case syncMapCommandLLen:
		return encodeToBytes(this.llenImpl(string(ss[1]), true, false))
	// Transaction (Lock / Unlock)
	case syncMapCommandLockAll:
		this.LockAll()
	case syncMapCommandUnlockAll:
		this.UnlockAll()
	case syncMapCommandIsLockedAll:
		return encodeToBytes(this.isLockedAllImpl(true))
	case syncMapCommandLockKey:
		this.LockKey(string(ss[1]))
	case syncMapCommandUnlockKey:
		this.UnlockKey(string(ss[1]))
	case syncMapCommandIsLockedKey:
		return encodeToBytes(this.isLockedKeyImpl(string(ss[1]), true))
	case syncMapCommandLIndex:
		key := string(ss[1]) // バイト列をそのまま保存するので そのまま使えない
		index := 0
		decodeFromBytes(ss[2], &index)
		elist, ok := this.loadDirect(key)
		list := elist.([][]byte)
		if !ok || index < 0 || index >= len(list) {
			panic(nil)
		}
		return list[index]
	case syncMapCommandLSet:
		index := 0
		decodeFromBytes(ss[2], &index)
		this.lsetImpl(string(ss[1]), index, ss[3], true, false)
	// Multi Command (for N+1)
	case syncMapCommandMGet:
		keys := splitBytesToStrs(ss[1])
		var result [][]byte
		for _, key := range keys {
			loaded, ok := this.loadDirect(key)
			if ok {
				result = append(result, loaded.([]byte))
			} else {
				result = append(result, []byte(""))
			}
		}
		return join(result)
	case syncMapCommandMSet:
		keys := splitBytesToStrs(ss[1])
		values := split(ss[2])
		for i, key := range keys {
			value := values[i]
			this.storeDirect(key, value)
		}
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
func (this *SyncMapServer) getImpl(key string, res interface{}, forceConnection bool) bool {
	if this.IsMasterServer() {
		return this.loadDirectWithDecoding(key, res)
	} else { // やっていき
		loadedBytes := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandGet),
				[]byte(key),
			})
		}, forceConnection)
		if len(loadedBytes) == 0 {
			return false
		}
		decodeFromBytes(loadedBytes, res)
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
func (this *SyncMapServer) setImpl(key string, value interface{}, forceConnection bool) {
	if this.IsMasterServer() {
		this.storeDirectWithEncoding(key, value)
	} else { // やっていき
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandSet),
				[]byte(key),
				encodeToBytes(value),
			})
		}, forceConnection)
	}
}
func (this *SyncMapServer) Set(key string, value interface{}) {
	this.setImpl(key, value, false)
}
func (this *SyncMapServerTransaction) Set(key string, value interface{}) {
	this.server.setImpl(key, value, true)
}

// 変更できるようにpointer型で受け取ること
// 一旦 MGetResult を経由することで、重複するキーのロードを一回のロードで済ませられる
type MGetResult struct {
	resultMap map[string][]byte
}

func newMGetResult() MGetResult {
	var result MGetResult
	result.resultMap = map[string][]byte{}
	return result
}

func (this MGetResult) Get(key string, value interface{}) bool {
	encoded, ok := this.resultMap[key]
	if !ok {
		return false
	}
	decodeFromBytes(encoded, value)
	return true
}
func (this *SyncMapServer) mgetImpl(keys []string, forceConnection bool) MGetResult {
	result := newMGetResult()
	if this.IsMasterServer() {
		for _, key := range keys {
			encoded, ok := this.loadDirect(key)
			if ok {
				result.resultMap[key] = encoded.([]byte)
			}
		}
	} else { // やっていき
		recieved := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandMGet),
				joinStrsToBytes(keys),
			})
		}, forceConnection)
		encodedValues := split(recieved)
		for i, encodedValue := range encodedValues {
			ok := encodedValue != nil && len(encodedValue) != 0
			if ok {
				result.resultMap[keys[i]] = encodedValue
			}
		}
	}
	return result
}

func (this *SyncMapServer) MGet(keys []string) MGetResult {
	return this.mgetImpl(keys, false)
}
func (this *SyncMapServerTransaction) MGet(keys []string) MGetResult {
	return this.server.mgetImpl(keys, true)
}

// encodeToBytes() した配列を渡すこと。
// Masterなら直に、SlaveならTCPでつないで実行
// あとでDecodeすること。空なら 空。
func (this *SyncMapServer) msetImpl(store map[string]interface{}, forceConnection bool) {
	if this.IsMasterServer() {
		for key, value := range store {
			this.storeDirectWithEncoding(key, value)
		}
	} else { // やっていき
		this.send(func() []byte {
			var savedValues [][]byte
			var keys []string
			for key, value := range store {
				keys = append(keys, key)
				savedValues = append(savedValues, encodeToBytes(value))
			}
			return join([][]byte{
				[]byte(syncMapCommandMSet),
				joinStrsToBytes(keys),
				join(savedValues),
			})
		}, forceConnection)
	}
}
func (this *SyncMapServer) MSet(store map[string]interface{}) {
	this.msetImpl(store, false)
}
func (this *SyncMapServerTransaction) MSet(store map[string]interface{}) {
	this.server.msetImpl(store, true)
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
	this.KeyCount = 0
	var mutex sync.Mutex
	this.mutex = mutex
	this.IsLocked = false
	// WARN: 接続しているコネクションは再設定したほうがよい？
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
		_, ok := this.loadDirect(key)
		return ok
	} else {
		encoded := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandExists),
				[]byte(key),
			})
		}, forceConnection)
		ok := false
		decodeFromBytes(encoded, &ok)
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
func (this *SyncMapServer) dbSizeImpl(forceDirect, forceConnection bool) int {
	if forceDirect || this.IsMasterServer() {
		return int(this.KeyCount)
	} else {
		encoded := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandDBSize),
			})
		}, forceConnection)
		result := 0
		decodeFromBytes(encoded, &result)
		return result
	}
}
func (this *SyncMapServer) DBSize() int {
	return this.dbSizeImpl(false, false)
}
func (this *SyncMapServerTransaction) DBSize() int {
	return this.server.dbSizeImpl(false, true)
}

// += value する
func (this *SyncMapServer) incrByImpl(key string, value int, forceDirect bool) int {
	if forceDirect || this.IsMasterServer() {
		this.LockAll()
		x := 0
		this.loadDirectWithDecoding(key, &x)
		x += value
		this.storeDirectWithEncoding(key, x)
		this.UnlockAll()
		return x
	} else { // やっていき
		x := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandIncrBy),
				[]byte(key),
				encodeToBytes(value),
			})
		}, false)
		result := 0
		decodeFromBytes(x, &result)
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
		decodeFromBytes(x, &result)
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
		decodeFromBytes(x, &result)
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

// List に要素を追加したのち index を返す
func (this *SyncMapServer) rpushImpl(key string, value interface{}, forceDirect, forceConnection bool) int {
	if forceDirect || this.IsMasterServer() {
		this.LockKey(key)
		elist, ok := this.loadDirect(key)
		if !ok { // そもそも存在しなかった時は追加
			this.storeDirect(key, [][]byte{encodeToBytes(value)})
			return 0
		}
		list := elist.([][]byte)
		if forceDirect {
			list = append(list, value.([]byte))
		} else {
			list = append(list, encodeToBytes(value))
		}
		this.storeDirect(key, list)
		this.UnlockKey(key)
		return len(list) - 1
	} else {
		encoded2 := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandRPush),
				[]byte(key),
				encodeToBytes(value),
			})
		}, forceConnection)
		x := 0
		decodeFromBytes(encoded2, &x)
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
		elist, ok := this.loadDirect(key)
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
		decodeFromBytes(encoded, &x)
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
		elist, ok := this.loadDirect(key)
		list := elist.([][]byte)
		if !ok || index < 0 || index >= len(list) {
			return false
		}
		decodeFromBytes(list[index], value)
		return true
	} else {
		encoded := this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandLIndex),
				[]byte(key),
				encodeToBytes(index),
			})
		}, forceConnection)
		if len(encoded) == 0 {
			return false
		}
		decodeFromBytes(encoded, value)
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
		elist, ok := this.loadDirect(key)
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
			list[index] = encodeToBytes(value)
		}
		this.storeDirect(key, list)
	} else {
		this.send(func() []byte {
			return join([][]byte{
				[]byte(syncMapCommandLSet),
				[]byte(key),
				encodeToBytes(index),
				encodeToBytes(value),
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
				for {
					writeAll(conn, this.interpretWrapFunction(readAll(conn)))
				}
				// PoolするのでconnectionはCloseさせない。
				// conn.Close()
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
	// WARN: Transaction時は強引に作成するので想定数よりも増えるので多めに確保
	this.connectionPool = make([]net.Conn, maxSyncMapServerConnectionNum*10)
	this.connectionPoolStatus = make([]int, maxSyncMapServerConnectionNum*10)
	this.connectionPoolEmptyIndexStack = stack.New()
	for i := 0; i < maxSyncMapServerConnectionNum; i++ {
		this.connectionPoolEmptyIndexStack.Push(i)
	}
	// 要求があって初めて接続する。再起動試験では起動順序が一律ではないため。
	// Redisがそういう仕組みなのでこちらもそのようにしておく
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
	var result [][][]byte
	this.SyncMap.Range(func(key, value interface{}) bool {
		var here [][]byte
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
	file.Write(encodeToBytes(result))
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
	var decoded [][][]byte
	decodeFromBytes(encoded, &decoded)
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
func (this *SyncMapServer) loadDirectWithDecoding(key string, res interface{}) bool {
	value, ok := this.loadDirect(key)
	if ok {
		decodeFromBytes(value.([]byte), res)
	}
	return ok
}
func (this *SyncMapServer) storeDirectWithEncoding(key string, value interface{}) {
	encoded := encodeToBytes(value)
	this.storeDirect(key, encoded)
}

// 集約させておくことで後で便利にする
func (this *SyncMapServer) loadDirect(key string) (interface{}, bool) {
	return this.SyncMap.Load(key)
}

// 自身の SyncMapにStore. value は []byte か [][]byte 型
func (this *SyncMapServer) storeDirect(key string, value interface{}) {
	_, exists := this.SyncMap.Load(key)
	if !exists {
		var m sync.Mutex
		this.mutexMap.Store(key, &m)
		this.lockedMap.Store(key, false)
		atomic.AddInt32(&this.KeyCount, 1)
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
	atomic.AddInt32(&this.KeyCount, -1)
}

// 生のbyteを送信
func (this *SyncMapServer) send(f func() []byte, force bool) []byte {
	if this.IsMasterServer() {
		return this.interpretWrapFunction(f())
	} else {
		return this.sendBySlave(f, force)
	}
}

// 仮の実装。全てが同じくらいの重さとした場合の理論値を出す
// -> スケールしてそう
var xxxMutexes = make([]sync.Mutex, maxSyncMapServerConnectionNum)
var xxxSum int32 = 0

func (this *SyncMapServer) sendBySlave(f func() []byte, force bool) []byte {
	if force { // TODO:
		log.Panic("Not Implementent Transaction")
	}
	// モデルとしては重いコネクションと軽いコネクションがあるはずなので。
	// 均等にするのではなく軽いやつは軽いやつでどんどんやってもらいたい
	// Lock()するので1台しか使わない実装 (速度が同じくらい出てほしいが、 findRunnableが遅いのでたいへん)
	// ゴルーチン生成コストは一瞬だが、 Lock() 待ちが多すぎて時間がかかっている
	poolIndex := atomic.AddInt32(&xxxSum, 1) % maxSyncMapServerConnectionNum
	xxxMutexes[poolIndex].Lock()
	// this.connectionPoolEmptyIndexStackMutex.Lock()
	// poolIndex := this.connectionPoolEmptyIndexStack.Pop().(int)
	poolStatus := this.connectionPoolStatus[poolIndex]
	conn := this.connectionPool[poolIndex]
	if poolStatus == ConnectionPoolStatusDisconnected {
		newConn, err := net.Dial("tcp", this.substanceAddress)
		if err != nil {
			fmt.Println("Client TCP Connect Error", err)
			if newConn != nil {
				newConn.Close()
			}
			time.Sleep(1 * time.Millisecond)
			this.connectionPoolStatus[poolIndex] = ConnectionPoolStatusDisconnected
			xxxMutexes[poolIndex].Unlock()
			// this.connectionPoolEmptyIndexStackMutex.Unlock()
			return this.sendBySlave(f, force)
		}
		conn = newConn
	}
	writeAll(conn, f())
	result := readAll(conn)
	this.connectionPool[poolIndex] = conn
	this.connectionPoolStatus[poolIndex] = ConnectionPoolStatusEmpty
	// this.connectionPoolEmptyIndexStack.Push(poolIndex)
	xxxMutexes[poolIndex].Unlock()
	// this.connectionPoolEmptyIndexStackMutex.Unlock()
	return result
}
