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

const defaultReadBufferSize = 8192                  // ガッと取ったほうが良い。メモリを使用したくなければ 1024.逆なら65536
const RedisHostPrivateIPAddress = "192.168.111.111" // ここで指定したサーバーに
// `NewSyncMapServer(GetMasterServerAddress()+":8884", MyServerIsOnMasterServerIP()) `
const SyncMapBackUpPath = "./syncmapbackup-" // カレントディレクトリにバックアップを作成。パーミッションに注意。
const DefaultBackUpTimeSecond = 30           // この秒数毎にバックアップファイルを作成する(デフォルトでBackUpが作成される設定)

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
	// データ毎に保存場所/コネクションを臨機応変に変えられるので分散しやすい.
	SyncMap   sync.Map // string -> (byte[] | byte[][])
	mutexMap  sync.Map // string -> *sync.Mutex
	lockedMap sync.Map // string -> bool
	keyCount  int32
	// 接続情報
	substanceAddress string
	masterPort       int
	// 同時にリクエストされるGoroutine の数がこれに比べて多いと性能が落ちる。
	// かといってものすごい多いと peer する. 16 ~ 100 くらいが安定か？
	// アクセス数に応じて調整すると吉
	maxSyncMapServerConnectionNum int
	// コネクションはプールして再利用する
	connectionPool                     [](*net.TCPConn)
	connectionPoolStatus               []int
	connectionPoolEmptyIndexStack      *stack.Stack
	connectionPoolEmptyIndexStackMutex sync.Mutex
	// 関数をカスタマイズする用.強引に複数台で同期したいときに便利。
	MySendCustomFunction func(this SyncMapServer, buf []byte) []byte
}

const ( // connectionPoolStatus
	ConnectionPoolStatusDisconnected = iota // = 0 未接続
	ConnectionPoolStatusUsing
	ConnectionPoolStatusEmpty
)

type SyncMapServerConn struct {
	server              *SyncMapServer
	connectionPoolIndex int      // (Transaction+Slave時) このコネクションを使える
	lockedKeys          []string // (Transaction時) これらのキーをロックしている
}

const NoConnectionIsSelected = -1

type KeyValueStoreConn interface { // ptr は参照を着けてLoadすることを示す
	// Normal Command
	Get(key string, value interface{}) bool // ptr (キーが無ければ false)
	Set(key string, value interface{})
	MGet(keys []string) MGetResult     // 改めて Get するときに ptr
	MSet(store map[string]interface{}) // 先に対応Mapを作りそれをMSet
	Exists(key string) bool
	Del(key string)
	IncrBy(key string, value int) int
	DBSize() int // means key count
	// Keys() []string TODO:
	FlushAll()
	// List 関連
	RPush(key string, value interface{}) int // Push後の 自身の index を返す
	LLen(key string) int
	LIndex(key string, index int, value interface{}) bool // ptr (キーが無ければ false)
	LSet(key string, index int, value interface{})
	// LRange(key string, start, stop int, values []interface{}) // ptr(0,-1 で全て取得可能) TODO:
	//  IsLocked(key string) は Redis には存在しない
}

type KeyValueStoreConnWithTransaction interface {
	KeyValueStoreConn
	Transaction(keys []string, f func()) (isok bool)
}

// 一旦 MGetResult を経由することで、重複するキーのロードを一回のロードで済ませられる
type MGetResult struct {
	resultMap map[string][]byte
}

// TODO: transaction中にno transactionなものが値を変えてくる可能性が十分にある
//       特に STORE / DELETE はやっかい。だが、たいていこれらはTransactionがついているはずなのでそこまで注意をしなくてもよいのではないか
const (
	syncMapCommandGet    = "G"      // get
	syncMapCommandMGet   = "MGET"   // multi get
	syncMapCommandSet    = "S"      // set
	syncMapCommandMSet   = "MSET"   // multi set
	syncMapCommandExists = "EXISTS" // check if exists key
	syncMapCommandDel    = "D"      // delete
	syncMapCommandIncrBy = "I"      // incrBy value
	syncMapCommandDBSize = "DBSIZE" // stored key count
	// list (内部的に([]byte ではなく [][]byte として保存しているので) Set / Get は使えない)
	// 順序が関係ないものに使うと吉
	syncMapCommandRPush  = "RPUSH"  // append value to list(最初が空でも可能)
	syncMapCommandLLen   = "LLEN"   // len of list
	syncMapCommandLIndex = "LINDEX" // get value from list
	syncMapCommandLSet   = "LSET"   // update value at index
	// 特定のキーをLockする。
	// それが解除されていれば、 特定のキーをロックする。
	syncMapCommandLockKey     = "LOCK_K"   // lock a key
	syncMapCommandUnlockKey   = "UNLOCK_K" // unlock a key
	syncMapCommandIsLockedKey = "ISLOCKED_K"
	// そのほか
	syncMapCommandFlushAll = "FLUSHALL"
	syncMapCommandCustom   = "CUSTOM" // custom
	// NOTE: LIST_GET_ALL 欲しい？
)

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
	if p, ok := x.(User); ok {
		byf, _ := p.Marshal([]byte{})
		return byf
	}
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
	if p, ok := x.(*User); ok {
		(*p).Unmarshal(input)
		return
	}
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
func sbytes(s string) []byte {
	return []byte(s)
	// NOTE: かなり　unsafe なやりかたなので落ちるかもしれないが高速化可能
	// return *(*[]byte)(unsafe.Pointer(&s))
}
func decodeBool(input []byte) bool {
	result := true
	decodeFromBytes(input, &result)
	return result
}
func decodeInt(input []byte) int {
	result := 0
	decodeFromBytes(input, &result)
	return result
}

// Sync Map Functions ///////////////////////////////////////////
// サーバーで受け取ってコマンドに対応する関数を実行
func (this SyncMapServerConn) interpretWrapFunction(buf []byte) []byte {
	input := split(buf)
	if len(input) < 1 {
		panic(nil)
	}
	switch string(input[0]) {
	// General Commands
	case syncMapCommandGet:
		return this.parseGet(input)
	case syncMapCommandSet:
		this.parseSet(input)
	case syncMapCommandMGet:
		return this.parseMGet(input)
	case syncMapCommandMSet:
		this.parseMSet(input)
	case syncMapCommandExists:
		return this.parseExists(input)
	case syncMapCommandDel:
		this.parseDel(input)
	case syncMapCommandIncrBy:
		return this.parseIncrBy(input)
	case syncMapCommandDBSize:
		return this.parseDBSize(input)
	// List Command
	case syncMapCommandRPush:
		return this.parseRPush(input)
	case syncMapCommandLLen:
		return this.parseLLen(input)
	case syncMapCommandLIndex:
		return this.parseLIndex(input)
	case syncMapCommandLSet:
		this.parseLSet(input)
	case syncMapCommandIsLockedKey:
		return this.parseIsLockedKey(input)
	case syncMapCommandLockKey:
		return this.parafreKey(input)
	case syncMapCommandUnlockKey:
		return this.parafreKey(input)
	// Custom Command
	case syncMapCommandCustom:
		return this.parseCustomFunction(input)
	case syncMapCommandFlushAll:
		this.FlushAll()
	default:
		panic(nil)
	}
	return []byte("")
}

// GET : 変更できるようにpointer型で受け取ること。
func (this SyncMapServerConn) Get(key string, res interface{}) bool {
	if this.IsMasterServer() {
		return this.loadDirectWithDecoding(key, res)
	}
	loadedBytes := this.send(syncMapCommandGet, []byte(key))
	if len(loadedBytes) == 0 {
		return false
	}
	decodeFromBytes(loadedBytes, res)
	return true
}
func (this SyncMapServerConn) parseGet(input [][]byte) []byte {
	key := string(input[1])
	value, ok := this.loadDirect(key)
	if ok {
		return value.([]byte)
	}
	return []byte{}
}

// SET
func (this SyncMapServerConn) Set(key string, value interface{}) {
	if this.IsMasterServer() {
		this.storeDirectWithEncoding(key, value)
	} else {
		this.send(syncMapCommandSet, []byte(key), encodeToBytes(value))
	}
}
func (this SyncMapServerConn) parseSet(input [][]byte) {
	this.storeDirect(string(input[1]), input[2])
}

// MGET : 変更できるようにpointer型で受け取ること
func (this SyncMapServerConn) MGet(keys []string, connIndex int) MGetResult {
	result := newMGetResult()
	if this.IsMasterServer() {
		for _, key := range keys {
			encoded, ok := this.loadDirect(key)
			if ok {
				result.resultMap[key] = encoded.([]byte)
			}
		}
	} else {
		recieved := this.send(syncMapCommandMGet, joinStrsToBytes(keys))
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
func (this SyncMapServerConn) parseMGet(input [][]byte) []byte {
	keys := splitBytesToStrs(input[1])
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

// MSET
func (this SyncMapServerConn) MSet(store map[string]interface{}) {
	if this.IsMasterServer() {
		for key, value := range store {
			this.storeDirectWithEncoding(key, value)
		}
	} else {
		var savedValues [][]byte
		var keys []string
		for key, value := range store {
			keys = append(keys, key)
			savedValues = append(savedValues, encodeToBytes(value))
		}
		this.send(syncMapCommandMSet, joinStrsToBytes(keys), join(savedValues))
	}
}
func (this SyncMapServerConn) parseMSet(input [][]byte) {
	keys := splitBytesToStrs(input[1])
	values := split(input[2])
	for i, key := range keys {
		value := values[i]
		this.storeDirect(key, value)
	}
}

// EXISTS
func (this SyncMapServerConn) Exists(key string) bool {
	if this.IsMasterServer() {
		_, ok := this.loadDirect(key)
		return ok
	} else {
		return decodeBool(this.send(syncMapCommandExists, []byte(key)))
	}
}
func (this SyncMapServerConn) parseExists(input [][]byte) []byte {
	return encodeToBytes(this.Exists(string(input[1])))
}

// DEL
func (this SyncMapServerConn) Del(key string) {
	if this.IsMasterServer() {
		this.deleteDirect(key)
	} else {
		this.send(syncMapCommandDel, []byte(key))
	}
}
func (this SyncMapServerConn) parseDel(input [][]byte) {
	this.Del(string(input[1]))
}

// INCRBY
// NOTE: Lockをするのでトランザクション時に呼び出すのはだめ
func (this SyncMapServerConn) IncrBy(key string, value int) int {
	if this.IsNowTransaction() {
		log.Panic("デッドロックするのでtransaction中に呼び出すのはやめてください！")
	}
	if this.IsMasterServer() {
		this.lockKey(key)
		x := 0
		this.loadDirectWithDecoding(key, &x)
		x += value
		this.storeDirectWithEncoding(key, x)
		this.unlockKey(key)
		return x
	}
	return decodeInt(this.send(syncMapCommandIncrBy, []byte(key), encodeToBytes(value)))
}
func (this SyncMapServerConn) parseIncrBy(input [][]byte) []byte {
	value := decodeInt(input[2])
	return encodeToBytes(this.IncrBy(string(input[1]), value))
}

// DBSIZE
func (this SyncMapServerConn) DBSize() int {
	if this.IsMasterServer() {
		return int(this.server.keyCount)
	} else {
		return decodeInt(this.send(syncMapCommandDBSize))
	}
}
func (this SyncMapServerConn) parseDBSize(input [][]byte) []byte {
	return encodeToBytes(this.DBSize())
}

// RPUSH :: List に要素を追加したのち index を返す
// NOTE: Lockをするのでトランザクション時に呼び出すのはだめ
func (this SyncMapServerConn) rpushImpl(key string, encodedValue []byte) int {
	this.lockKey(key)
	elist, ok := this.loadDirect(key)
	if !ok { // そもそも存在しなかった時は追加
		this.storeDirect(key, [][]byte{encodedValue})
		return 0
	}
	list := append(elist.([][]byte), encodedValue)
	this.storeDirect(key, list)
	this.unlockKey(key)
	return len(list) - 1
}
func (this SyncMapServerConn) RPush(key string, value interface{}) int {
	if this.IsMasterServer() {
		return this.rpushImpl(key, encodeToBytes(value))
	} else {
		return decodeInt(this.send(syncMapCommandRPush, []byte(key), encodeToBytes(value)))
	}
}
func (this SyncMapServerConn) parseRPush(input [][]byte) []byte {
	return encodeToBytes(this.rpushImpl(string(input[1]), input[2]))
}

// LLEN: list のサイズを返す
func (this SyncMapServerConn) LLen(key string) int {
	if this.IsMasterServer() {
		elist, ok := this.loadDirect(key)
		if !ok {
			return 0
		}
		return len(elist.([][]byte))
	} else {
		return decodeInt(this.send(syncMapCommandLLen, []byte(key)))
	}
}
func (this SyncMapServerConn) parseLLen(input [][]byte) []byte {
	return encodeToBytes(this.LLen(string(input[1])))
}

// LINDEX: 変更できるようにpointer型で受け取ること
func (this SyncMapServerConn) LIndex(key string, index int, value interface{}) bool {
	if this.IsMasterServer() {
		elist, ok := this.loadDirect(key)
		list := elist.([][]byte)
		if !ok || index < 0 || index >= len(list) {
			return false
		}
		decodeFromBytes(list[index], value)
		return true
	} else {
		encoded := this.send(syncMapCommandLIndex, []byte(key), encodeToBytes(index))
		if len(encoded) == 0 {
			return false
		}
		decodeFromBytes(encoded, value)
		return true
	}
}
func (this SyncMapServerConn) parseLIndex(input [][]byte) []byte {
	key := string(input[1])
	index := 0
	decodeFromBytes(input[2], &index)
	elist, ok := this.loadDirect(key)
	list := elist.([][]byte)
	if !ok || index < 0 || index >= len(list) {
		panic(nil)
	}
	return list[index]
}

// LSet: List を Update する
func (this SyncMapServerConn) lsetImpl(key string, index int, encodedValue []byte) {
	elist, ok := this.loadDirect(key)
	list := elist.([][]byte)
	if !ok || index < 0 || index >= len(list) {
		panic(nil)
	}
	list[index] = encodedValue
	this.storeDirect(key, list)
}
func (this SyncMapServerConn) LSet(key string, index int, value interface{}) {
	if this.IsMasterServer() {
		this.lsetImpl(key, index, encodeToBytes(value))
	} else {
		this.send(syncMapCommandLSet, []byte(key), encodeToBytes(index), encodeToBytes(value))
	}
}
func (this SyncMapServerConn) parseLSet(input [][]byte) {
	index := 0
	decodeFromBytes(input[2], &index)
	this.lsetImpl(string(input[1]), index, input[3])
}

// トランザクション
func (this SyncMapServerConn) IsLockedKey(key string) bool {
	if this.IsMasterServer() {
		locked, ok := this.server.lockedMap.Load(key)
		if !ok {
			return false // 存在しない == ロックされていない
		}
		return locked.(bool)
	} else {
		return decodeBool(this.send(syncMapCommandIsLockedKey, []byte(key)))
	}
}
func (this SyncMapServerConn) parseIsLockedKey(input [][]byte) []byte {
	return encodeToBytes(this.IsLockedKey(string(input[1])))
}
// キーはソート済みを想定
func (this SyncMapServerConn) lockKeys(keys []string) {
	m, ok := this.server.mutexMap.Load(key)
	if !ok {
		// ここで作成してしまうと二つのLockが競合するので,Store時にだけ生成されるようにする
		log.Panic("存在しないキー" + key + "へのロックが掛かりました")
	}
	m.(*sync.Mutex).Lock()
	this.server.lockedMap.Store(key, true)
}
// キーはソート済みを想定
func (this SyncMapServerConn) unlockKeys(key []string) {
	m, ok := this.server.mutexMap.Load(key)
	if !ok {
		log.Panic("存在しないキー" + key + "へのアンロックが掛かりました")
	}
	this.server.lockedMap.Store(key, false)
	m.(*sync.Mutex).Unlock()
}

// TODO: キーをソートしたりロックしたりコネクションを専有したり
func (this SyncMapServerConn) Transaction(keys []string, f func()) (isok bool) {
	if this.IsMasterServer() {
		this.lockKey(keys)
		f()
		this.unlockKey(keys)
	} else {
		keysEncoded := joinStrsToBytes(keys)
		this.send(syncMapCommandLockKey, keysEncoded)
		f()
		this.send(syncMapCommandUnlockKey, keysEncoded)
	}
}

// 自作関数を使用する時用
func DefaultSendCustomFunction(this SyncMapServerConn, buf []byte) []byte {
	return buf // echo server
}
func (this SyncMapServerConn) CustomFunction(f func() []byte) []byte {
	if this.IsMasterServer() {
		return this.server.MySendCustomFunction(this, f())
	} else {
		return this.send(syncMapCommandCustom, f())
	}
}
func (this SyncMapServerConn) parseCustomFunction(input [][]byte) []byte {
	return this.CustomFunction(func() []byte { return input[1] })
}

// 全ての要素を削除する
func (this SyncMapServerConn) FlushAll() {
	if this.IsMasterServer() {
		var syncMap sync.Map
		this.SyncMap = syncMap
		var mutexMap sync.Map
		this.mutexMap = mutexMap
		var lockedMap sync.Map
		this.lockedMap = lockedMap
		this.keyCount = 0
		var mutex sync.Mutex
		this.mutex = mutex
		this.IsLocked = false
	} else {
		this.send(syncMapCommandFlushAll)
	}
}

//  SyncMap で使用する関数
func NewSyncMapServer(substanceAddress string, isMaster bool) SyncMapServer {
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
func (this SyncMapServer) GetConn() SyncMapServerConn {
	return SyncMapServerConn{
		server:              *this,
		connectionPoolIndex: NoConnectionIsSelected,
	}
}
func (this SyncMapServerConn) IsMasterServer() bool {
	return len(this.server.substanceAddress) == 0
}
func (this SyncMapServerConn) IsNowTransaction() bool {
	return len(this.lockedKeys) > 0
}
func newMasterSyncMapServer(port int) SyncMapServer {
	this := SyncMapServer{}
	this.substanceAddress = ""
	this.masterPort = port
	go func() {
		tcpAddr, err := net.ResolveTCPAddr("tcp4", "0.0.0.0:"+strconv.Itoa(port))
		if err != nil {
			log.Panic("net cannot resolve TCP Addr error ", err)
		}
		listen, err := net.ListenTCP("tcp", tcpAddr)
		defer listen.Close()
		if err != nil {
			panic(err)
		}
		for {
			conn, err := listen.AcceptTCP()
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
	this.MySendCustomFunction = func(this SyncMapServer, buf []byte) []byte { return buf }
	// バックアップファイルが見つかればそれを読み込む
	this.readFile()
	// バックアッププロセスを開始する
	this.startBackUpProcess()
	return this
}
func newSlaveSyncMapServer(substanceAddress string) SyncMapServer {
	this := SyncMapServer{}
	this.substanceAddress = substanceAddress
	port, err := strconv.Atoi(strings.Split(substanceAddress, ":")[1])
	if err != nil {
		panic(err)
	}
	this.masterPort = port
	this.MySendCustomFunction = func(this SyncMapServer, buf []byte) []byte { return buf }
	// WARN: Transaction時は強引に作成するので想定数よりも増えるので多めに確保
	this.connectionPool = make([]*net.TCPConn, maxSyncMapServerConnectionNum*10)
	this.connectionPoolStatus = make([]int, maxSyncMapServerConnectionNum*10)
	this.connectionPoolEmptyIndexStack = stack.New()
	for i := 0; i < maxSyncMapServerConnectionNum; i++ {
		this.connectionPoolEmptyIndexStack.Push(i)
	}
	// 要求があって初めて接続する。再起動試験では起動順序が一律ではないため。
	// Redisがそういう仕組みなのでこちらもそのようにしておく
	return this
}
func (this SyncMapServer) getPath() string {
	return SyncMapBackUpPath + strconv.Itoa(this.masterPort) + ".sm"
}
func (this SyncMapServer) writeFile() {
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
func (this SyncMapServer) readFile() {
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
func (this SyncMapServer) startBackUpProcess() {
	go func() {
		time.Sleep(time.Duration(DefaultBackUpTimeSecond) * time.Second)
		this.writeFile()
	}()
}

// 自身の SyncMapからLoad / 変更できるようにpointer型で受け取ること
func (this SyncMapServerConn) loadDirectWithDecoding(key string, res interface{}) bool {
	value, ok := this.loadDirect(key)
	if ok {
		decodeFromBytes(value.([]byte), res)
	}
	return ok
}
func (this SyncMapServerConn) storeDirectWithEncoding(key string, value interface{}) {
	encoded := encodeToBytes(value)
	this.storeDirect(key, encoded)
}

// 集約させておくことで後で便利にする
func (this SyncMapServerConn) loadDirect(key string) (interface{}, bool) {
	x, ok := this.server.SyncMap.Load(key)
	return x, ok
}

// 自身の SyncMapにStore. value は []byte か [][]byte 型
func (this SyncMapServerConn) storeDirect(key string, value interface{}) {
	_, exists := this.server.SyncMap.Load(key)
	if !exists {
		var m sync.Mutex
		this.server.mutexMap.Store(key, &m)
		this.server.lockedMap.Store(key, false)
		atomic.AddInt32(&this.server.keyCount, 1)
	}
	this.server.SyncMap.Store(key, value)
}
func (this SyncMapServerConn) deleteDirect(key string) {
	_, exists := this.server.SyncMap.Load(key)
	if !exists {
		return
	}
	this.server.SyncMap.Delete(key)
	this.server.mutexMap.Delete(key)
	this.server.lockedMap.Delete(key)
	atomic.AddInt32(&this.server.keyCount, -1)
}

// 生のbyteを送信
func (this SyncMapServerConn) send(command string, packet ...[]byte) []byte {
	encoded := join([][]byte{
		[]byte(command),
		packet,
	})
	if this.IsMasterServer() {
		return this.interpretWrapFunction(encoded)
	} else {
		return this.sendBySlave(encoded)
	}
}
func (this SyncMapServerConn) sendBySlave(packet []byte) []byte {
	if force { // TODO:
		log.Panic("Not Implementent Transaction")
	}
	findEmptyConnection := func() int {
		sleep := func() {
			time.Sleep(500 * time.Nanosecond)
		}
		for {
			// Lock とかするまえにそもそも 0 なら望みがない
			if this.connectionPoolEmptyIndexStack.Len() == 0 {
				sleep()
				continue
			}
			this.connectionPoolEmptyIndexStackMutex.Lock()
			if this.connectionPoolEmptyIndexStack.Len() == 0 {
				this.connectionPoolEmptyIndexStackMutex.Unlock()
				sleep()
				continue
			}
			result := this.connectionPoolEmptyIndexStack.Pop().(int)
			this.connectionPoolEmptyIndexStackMutex.Unlock()
			return result
		}
	}
	poolIndex := findEmptyConnection()
	poolStatus := this.connectionPoolStatus[poolIndex]
	conn := this.connectionPool[poolIndex]
	if poolStatus == ConnectionPoolStatusDisconnected {
		tcpAddr, err := net.ResolveTCPAddr("tcp4", this.substanceAddress)
		if err != nil {
			log.Panic("net resolve TCP Addr error ", err)
		}
		newConn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			fmt.Println("Client TCP Connect Error", err)
			if newConn != nil {
				newConn.Close()
			}
			time.Sleep(1 * time.Millisecond)
			this.connectionPoolStatus[poolIndex] = ConnectionPoolStatusDisconnected
			return this.sendBySlave(packet, force)
		}
		// NOTE: できるなら永遠に接続したい
		newConn.SetKeepAlive(true)
		// newConn.SetReadBuffer(65536)
		// newConn.SetWriteBuffer(65536)
		conn = newConn
	}
	writeAll(conn, packet)
	result := readAll(conn)
	this.connectionPool[poolIndex] = conn
	this.connectionPoolStatus[poolIndex] = ConnectionPoolStatusEmpty
	this.connectionPoolEmptyIndexStackMutex.Lock()
	this.connectionPoolEmptyIndexStack.Push(poolIndex)
	this.connectionPoolEmptyIndexStackMutex.Unlock()
	return result
}
