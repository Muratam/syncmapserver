package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shamaton/msgpack"
)

// 同時にリクエストされるGoroutine の数がこれに比べて多いと性能が落ちる。
// かといってものすごい多いと peer する. 16 ~ 100 くらいが安定か？アクセス過多な場合は仕方ない。
const maxSyncMapServerConnectionNum = 50
const defaultReadBufferSize = 8192                 // ガッと取ったほうが良い。メモリを使用したくなければ 1024.逆なら65536
const RedisHostPrivateIPAddress = "172.24.122.185" // ここで指定したサーバーに(Redis /SyncMapServerを) 建てる
// `NewSyncMapServerConn(GetMasterServerAddress()+":8884", MyServerIsOnMasterServerIP()) `
const SyncMapBackUpPath = "./syncmapbackup-" // カレントディレクトリにバックアップを作成。パーミッションに注意。
const InitMarkPath = "./init-"               // 初期化データ
// 起動後この秒数毎にバックアップファイルを作成する(デフォルトでBackUpが作成される設定)
// /initialize の 120秒後にBackUpとかが多分いい感じかも。
// Redis は save 900 1 \n save 300 10 \n save 60 10000 とかを手動で設定ファイルに書くとよさそう
const DefaultBackUpTimeSecond = 120

// 一人がロック中に他のロックしていない人が値を書き換えることができるが問題はないはず
//  ↑ 整合性が必要なデータかつ不必要なデータということになるので、そんなことは起こらないはず

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
	// コネクションはプールして再利用する
	connectionPool             [](*net.TCPConn)
	connectionPoolStatus       []int
	connectionPoolEmptyChannel chan int
	// 関数をカスタマイズする用.強引に複数台で同期したいときに便利。
	MySendCustomFunction func(this *SyncMapServerConn, buf []byte) []byte
	// 初期化の方法を記す。 .Initialize  が呼ばれた時にこれで初期化する
	// InitMarkPath(./init-) があればそれを読んで初期化関数は無視するし、なければ初期化関数を実行する。
	InitializeFunction func()
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
	DBSize() int       // means key count
	AllKeys() []string // get all keys
	FlushAll()
	// List 関連
	RPush(key string, values ...interface{}) int // Push後の最後の要素の index を返す
	LLen(key string) int
	LIndex(key string, index int, value interface{}) bool // ptr (キーが無ければ false)
	LPop(key string, value interface{}) bool              // ptr (キーが無ければ false)
	RPop(key string, value interface{}) bool              // ptr (キーが無ければ false)
	LSet(key string, index int, value interface{})
	LRange(key string, startIndex, stopIncludingIndex int) LRangeResult // ptr (0,-1 で全て取得可能) (負数の場合はPythonと同じような処理(stopIncludingIndexがPythonより1多い)) [a,b,c][0:-1] はPythonでは最後を含まないがこちらは含む
	// IsLocked(key string) は Redis には存在しない
	Transaction(key string, f func(tx KeyValueStoreConn)) (isok bool)
	TransactionWithKeys(keys []string, f func(tx KeyValueStoreConn)) (isok bool)
	// ISUCONで初期化の負荷を軽減するために使う
	Initialize()
}

// 一旦 MGetResult を経由することで、重複するキーのロードを一回のロードで済ませられる
type MGetResult struct {
	resultMap map[string][]byte
}
type LRangeResult struct {
	resultArray [][]byte
}

const ( // COMMANDS
	syncMapCommandGet     = "G"       // get
	syncMapCommandMGet    = "MGET"    // multi get
	syncMapCommandSet     = "S"       // set
	syncMapCommandMSet    = "MSET"    // multi set
	syncMapCommandExists  = "E"       // check if exists key
	syncMapCommandDel     = "D"       // delete
	syncMapCommandIncrBy  = "I"       // incrBy value
	syncMapCommandDBSize  = "L"       // stored key count
	syncMapCommandAllKeys = "ALLKEYS" // get all keys
	// list (内部的に([]byte ではなく [][]byte として保存している))
	// 順序が関係ないものに使うと吉
	syncMapCommandRPush  = "RPUSH"  // append value to list(最初が空でも可能)
	syncMapCommandLLen   = "LLEN"   // len of list
	syncMapCommandLIndex = "LINDEX" // get value from list
	syncMapCommandRPop   = "RPOP"   // pop last
	syncMapCommandLPop   = "LPOP"   // pop head
	syncMapCommandLSet   = "LSET"   // update value at index
	syncMapCommandLRange = "LRANGE" // get list of range
	// 特定のキーをLockする。
	// それが解除されていれば、 特定のキーをロックする。
	syncMapCommandLockKey     = "LL" // lock a key
	syncMapCommandUnlockKey   = "LU" // unlock a key
	syncMapCommandIsLockedKey = "LI" // check is locked key
	// そのほか
	syncMapCommandFlushAll   = "FLUSHALL"
	syncMapCommandCustom     = "CUSTOM" // custom
	syncMapCommandInitialize = "INITIALIZE"
	// check lock
	syncMapCommandIncrByWithLock = "I_WL"
	syncMapCommandRPushWithLock  = "RPUSH_WL"
	syncMapCommandLPopWithLock   = "LPOP_WL"
	syncMapCommandRPopWithLock   = "RPOP_WL"
)

// bytes utils // Connection Pool のために Contents長さを指定する変換が入る
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
	var bufAll []byte
	readMax := defaultReadBufferSize
	currentReadLen := 0
	// 検証結果より
	// 2回 Read を呼び出しているが、適切なサイズの buffer を確保できて効率が良いため、
	// Read を 1回だけに絞るよりも 1回目でサイズを固定して 2回から適切にやっていくほうが効率がよい
	if true {
		buf := make([]byte, 4)
		readBufNum, err := conn.Read(buf)
		if readBufNum != 4 {
			if readBufNum == 0 {
				return readAll(conn)
			} else {
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
		bufAll = make([]byte, contentLen)
	}
	for currentReadLen < contentLen {
		readLen := min(readMax, contentLen-currentReadLen)
		buf := make([]byte, readLen)
		readBufNum, err := conn.Read(buf)
		if err != nil && err != io.EOF {
			log.Panic(err)
		}
		if readBufNum == 0 {
			continue
		}
		for i := 0; i < readBufNum; i++ {
			bufAll[i+currentReadLen] = buf[i]
		}
		currentReadLen += readBufNum
		if err == io.EOF {
			if currentReadLen+readBufNum != contentLen {
				log.Panic("invalid len TCP")
			}
			return bufAll
		}
	}
	if currentReadLen != contentLen {
		log.Panic("invalid len TPC !!")
	}
	return bufAll
}
func writeAll(conn net.Conn, content []byte) {
	contentLen := len(content)
	if contentLen >= 4294967296 {
		log.Panic("Too Long Content", contentLen)
	}
	n := contentLen + 4
	packet := make([]byte, n)
	size := format32bit(contentLen)
	packet[0] = size[0]
	packet[1] = size[1]
	packet[2] = size[2]
	packet[3] = size[3]
	for i := 4; i < n; i++ {
		packet[i] = content[i-4]
	}
	conn.Write(packet)
}

// []byte
// <-> interface{} :: encodeToBytes  <-> decodeFromBytes
// <-> [][]byte    :: join           <-> split
// <-> []string    :: joinStrsToBytes <-> splitBytesToStrs
// <-> string      :: byte[]()       <-> string()
//
// 変更できるようにpointer型で受け取ること
func decodeFromBytes(input []byte, x interface{}) {
	if x == nil {
		return
	}
	msgpack.Decode(input, x)
}

// 240 - 17
func encodeToBytes(x interface{}) []byte {
	d, _ := msgpack.Encode(&x)
	return d
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
	// かなりunsafe なやりかたなので落ちるかもしれないがbyteの変換は更に高速化可能
	// return *(*[]byte)(unsafe.Pointer(&s))
	// EncodeToBytes / DecodeFromBytes のほうが強すぎて意味がない
	return []byte(s)
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
func (this *SyncMapServerConn) interpretWrapFunction(buf []byte) []byte {
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
	case syncMapCommandIncrByWithLock:
		return this.parseIncrByWithLock(input)
	case syncMapCommandDBSize:
		return this.parseDBSize(input)
	case syncMapCommandAllKeys:
		return this.parseAllKeys()
	// List Command
	case syncMapCommandRPush:
		return this.parseRPush(input)
	case syncMapCommandRPushWithLock:
		return this.parseRPushWithLock(input)
	case syncMapCommandLLen:
		return this.parseLLen(input)
	case syncMapCommandLIndex:
		return this.parseLIndex(input)
	case syncMapCommandLPop:
		return this.parseLPop(input)
	case syncMapCommandLPopWithLock:
		return this.parseLPopWithLock(input)
	case syncMapCommandRPop:
		return this.parseRPop(input)
	case syncMapCommandRPopWithLock:
		return this.parseRPopWithLock(input)
	case syncMapCommandLSet:
		this.parseLSet(input)
	case syncMapCommandLRange:
		return this.parseLRange(input)
	case syncMapCommandIsLockedKey:
		return this.parseIsLockedKey(input)
	case syncMapCommandLockKey:
		this.parseLockKeys(input)
	case syncMapCommandUnlockKey:
		this.parseUnlockKeys(input)
	// Custom Command
	case syncMapCommandCustom:
		return this.parseCustomFunction(input)
	case syncMapCommandInitialize:
		this.Initialize()
	case syncMapCommandFlushAll:
		this.FlushAll()
	default:
		panic(nil)
	}
	return []byte("")
}

// GET : 変更できるようにpointer型で受け取ること。
func (this *SyncMapServerConn) Get(key string, res interface{}) bool {
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
func (this *SyncMapServerConn) parseGet(input [][]byte) []byte {
	key := string(input[1])
	value, ok := this.loadDirect(key)
	if ok {
		return value.([]byte)
	}
	return []byte("")
}

// SET
func (this *SyncMapServerConn) Set(key string, value interface{}) {
	if this.IsMasterServer() {
		this.storeDirectWithEncoding(key, value)
	} else {
		this.send(syncMapCommandSet, []byte(key), encodeToBytes(value))
	}
}
func (this *SyncMapServerConn) parseSet(input [][]byte) {
	this.storeDirect(string(input[1]), input[2])
}

// MGET : 変更できるようにpointer型で受け取ること
func (this *SyncMapServerConn) MGet(keys []string) MGetResult {
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
func (this *SyncMapServerConn) parseMGet(input [][]byte) []byte {
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
func (this *MGetResult) Get(key string, value interface{}) bool {
	encoded, ok := this.resultMap[key]
	if !ok {
		return false
	}
	decodeFromBytes(encoded, value)
	return true
}

// MSET
func (this *SyncMapServerConn) MSet(store map[string]interface{}) {
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
func (this *SyncMapServerConn) parseMSet(input [][]byte) {
	keys := splitBytesToStrs(input[1])
	values := split(input[2])
	for i, key := range keys {
		value := values[i]
		this.storeDirect(key, value)
	}
}

// EXISTS
func (this *SyncMapServerConn) Exists(key string) bool {
	if this.IsMasterServer() {
		_, ok := this.loadDirect(key)
		return ok
	} else {
		return decodeBool(this.send(syncMapCommandExists, []byte(key)))
	}
}
func (this *SyncMapServerConn) parseExists(input [][]byte) []byte {
	return encodeToBytes(this.Exists(string(input[1])))
}

// DEL
func (this *SyncMapServerConn) Del(key string) {
	if this.IsMasterServer() {
		this.deleteDirect(key)
	} else {
		this.send(syncMapCommandDel, []byte(key))
	}
}
func (this *SyncMapServerConn) parseDel(input [][]byte) {
	this.Del(string(input[1]))
}

// INCRBY
func (this *SyncMapServerConn) incrByImpl(key string, value int, needLock bool) int {
	conn := this
	if needLock {
		conn = this.New()
		conn.lockKeysDirect([]string{key})
	}
	x := 0
	conn.loadDirectWithDecoding(key, &x)
	x += value
	conn.storeDirectWithEncoding(key, x)
	if needLock {
		conn.unlockKeysDirect([]string{key})
	}
	return x
}
func (this *SyncMapServerConn) IncrBy(key string, value int) int {
	needLock := !this.myConnectionIsLocking(key)
	if this.IsMasterServer() {
		return this.incrByImpl(key, value, needLock)
	} else {
		command := syncMapCommandIncrBy
		if needLock {
			command = syncMapCommandIncrByWithLock
		}
		return decodeInt(this.send(command, []byte(key), encodeToBytes(value)))
	}
}
func (this *SyncMapServerConn) parseIncrBy(input [][]byte) []byte {
	return encodeToBytes(this.incrByImpl(string(input[1]), decodeInt(input[2]), false))
}
func (this *SyncMapServerConn) parseIncrByWithLock(input [][]byte) []byte {
	return encodeToBytes(this.incrByImpl(string(input[1]), decodeInt(input[2]), true))
}

// DBSIZE
func (this *SyncMapServerConn) DBSize() int {
	if this.IsMasterServer() {
		return int(this.server.keyCount)
	} else {
		return decodeInt(this.send(syncMapCommandDBSize))
	}
}
func (this *SyncMapServerConn) parseDBSize(input [][]byte) []byte {
	return encodeToBytes(this.DBSize())
}

// ALLKEYS
func (this *SyncMapServerConn) AllKeys() []string {
	if this.IsMasterServer() {
		result := make([]string, 0)
		this.server.SyncMap.Range(func(key, value interface{}) bool {
			result = append(result, key.(string))
			return true
		})
		return result
	} else {
		return splitBytesToStrs(this.send(syncMapCommandAllKeys))
	}
}
func (this *SyncMapServerConn) parseAllKeys() []byte {
	keys := this.AllKeys()
	return joinStrsToBytes(keys)
}

// RPUSH :: List に要素を追加したのち index を返す
func (this *SyncMapServerConn) rpushImpl(key string, joinedValues []byte, needLock bool) int {
	// Encode して join したものを受け取る
	conn := this
	if needLock {
		conn = this.New()
		conn.lockKeysDirect([]string{key})
		defer conn.unlockKeysDirect([]string{key})
	}
	values := split(joinedValues)
	elist, ok := conn.loadDirect(key)
	if !ok { // そもそも存在しなかった時は追加
		this.storeDirect(key, values)
		return len(values) - 1
	}
	list := append(elist.([][]byte), values...)
	conn.storeDirect(key, list)
	return len(list) - 1
}
func (this *SyncMapServerConn) RPush(key string, values ...interface{}) int {
	needLock := !this.myConnectionIsLocking(key)
	joiningValues := make([][]byte, 0)
	for _, value := range values {
		joiningValues = append(joiningValues, encodeToBytes(value))
	}
	if this.IsMasterServer() {
		return this.rpushImpl(key, join(joiningValues), needLock)
	} else {
		command := syncMapCommandRPush
		if needLock {
			command = syncMapCommandRPushWithLock
		}
		return decodeInt(this.send(command, []byte(key), join(joiningValues)))
	}
}
func (this *SyncMapServerConn) parseRPush(input [][]byte) []byte {
	return encodeToBytes(this.rpushImpl(string(input[1]), input[2], false))
}
func (this *SyncMapServerConn) parseRPushWithLock(input [][]byte) []byte {
	return encodeToBytes(this.rpushImpl(string(input[1]), input[2], true))
}

// LLEN: list のサイズを返す
func (this *SyncMapServerConn) LLen(key string) int {
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
func (this *SyncMapServerConn) parseLLen(input [][]byte) []byte {
	return encodeToBytes(this.LLen(string(input[1])))
}

// LINDEX: 変更できるようにpointer型で受け取ること
func (this *SyncMapServerConn) LIndex(key string, index int, value interface{}) bool {
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
func (this *SyncMapServerConn) parseLIndex(input [][]byte) []byte {
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

// LPOP/RPOP 変更できるようにpointer型で受け取ること
func (this *SyncMapServerConn) popImpl(key string, needLock, isPopHead bool) []byte {
	// Encode して join したものを受け取る
	conn := this
	if needLock {
		conn = this.New()
		conn.lockKeysDirect([]string{key})
		defer conn.unlockKeysDirect([]string{key})
	}
	elist, ok := conn.loadDirect(key)
	if !ok {
		return []byte{}
	}
	list := elist.([][]byte)
	if len(list) == 0 {
		return []byte{}
	}
	var result []byte
	if isPopHead {
		result = list[0]
		list = list[1:]
	} else {
		result = list[len(list)-1]
		list = list[:len(list)-1]
	}
	conn.storeDirect(key, list)
	return result
}
func (this *SyncMapServerConn) popWrap(key string, value interface{}, isPopHead bool) bool {
	needLock := !this.myConnectionIsLocking(key)
	if this.IsMasterServer() {
		encoded := this.popImpl(key, needLock, isPopHead)
		if len(encoded) == 0 {
			return false
		}
		decodeFromBytes(encoded, value)
		return true
	} else {
		command := ""
		if needLock {
			if isPopHead {
				command = syncMapCommandLPopWithLock
			} else {
				command = syncMapCommandRPopWithLock
			}
		} else {
			if isPopHead {
				command = syncMapCommandLPop
			} else {
				command = syncMapCommandRPop
			}
		}
		result := false
		decodeFromBytes(this.send(command, []byte(key)), &value)
		return result
	}
}
func (this *SyncMapServerConn) LPop(key string, value interface{}) bool {
	return this.popWrap(key, value, true)
}
func (this *SyncMapServerConn) parseLPop(input [][]byte) []byte {
	return this.popImpl(string(input[1]), false, true)
}
func (this *SyncMapServerConn) parseLPopWithLock(input [][]byte) []byte {
	return this.popImpl(string(input[1]), true, true)
}
func (this *SyncMapServerConn) RPop(key string, value interface{}) bool {
	return this.popWrap(key, value, false)
}
func (this *SyncMapServerConn) parseRPop(input [][]byte) []byte {
	return this.popImpl(string(input[1]), false, false)
}
func (this *SyncMapServerConn) parseRPopWithLock(input [][]byte) []byte {
	return this.popImpl(string(input[1]), true, false)
}

// LSet: List を Update する
func (this *SyncMapServerConn) lsetImpl(key string, index int, encodedValue []byte) {
	elist, ok := this.loadDirect(key)
	list := elist.([][]byte)
	if !ok || index < 0 || index >= len(list) {
		panic(nil)
	}
	list[index] = encodedValue
	this.storeDirect(key, list)
}
func (this *SyncMapServerConn) LSet(key string, index int, value interface{}) {
	if this.IsMasterServer() {
		this.lsetImpl(key, index, encodeToBytes(value))
	} else {
		this.send(syncMapCommandLSet, []byte(key), encodeToBytes(index), encodeToBytes(value))
	}
}
func (this *SyncMapServerConn) parseLSet(input [][]byte) {
	index := 0
	decodeFromBytes(input[2], &index)
	this.lsetImpl(string(input[1]), index, input[3])
}

// LRANGE: 範囲指定してリストを取得
func (this *SyncMapServerConn) lrangeImpl(key string, startIndex, stopIncludingIndex int) [][]byte {
	elist, ok := this.loadDirect(key)
	list := elist.([][]byte)
	if !ok {
		return [][]byte{}
	}
	parse := func(i int) int {
		if i >= 0 {
			return i
		}
		return len(list) + i
	}
	// including
	iStart := -1
	iEnd := -1
	for i := parse(startIndex); i <= parse(stopIncludingIndex); i++ {
		if i < 0 {
			i = -1
			continue
		}
		if i >= len(list) {
			break
		}
		if iStart < 0 {
			iStart = i
		}
		iEnd = i
	}
	if iStart < 0 || iEnd < 0 || iEnd < iStart {
		return [][]byte{}
	}
	return list[iStart : iEnd+1]
}
func (this *SyncMapServerConn) LRange(key string, startIndex, stopIncludingIndex int) LRangeResult {
	if this.IsMasterServer() {
		return NewLRangeResult(this.lrangeImpl(key, startIndex, stopIncludingIndex))
	} else {
		encoded := this.send(syncMapCommandLRange, []byte(key), encodeToBytes(startIndex), encodeToBytes(stopIncludingIndex))
		if len(encoded) == 0 {
			return NewLRangeResult([][]byte{})
		}
		return NewLRangeResult(split(encoded))
	}
}
func (this *SyncMapServerConn) parseLRange(input [][]byte) []byte {
	key := string(input[1])
	startIndex := decodeInt(input[2])
	stopIncludingIndex := decodeInt(input[3])
	return join(this.lrangeImpl(key, startIndex, stopIncludingIndex))
}

func NewLRangeResult(input [][]byte) LRangeResult {
	return LRangeResult{resultArray: input}
}

// 値は ptr で取得すること
func (this *LRangeResult) Get(index int, value interface{}) {
	if index < 0 || index >= len(this.resultArray) {
		log.Panic("Invalid Index For LGET")
	}
	decodeFromBytes(this.resultArray[index], value)
}
func (this *LRangeResult) Len() int {
	return len(this.resultArray)
}

// func (this *SyncMapServerConn) LRange(key string, startIndex, stopIncludingIndex int, values []interface{}) {
// }

// トランザクション
func (this *SyncMapServerConn) IsLockedKey(key string) bool {
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
func (this *SyncMapServerConn) parseIsLockedKey(input [][]byte) []byte {
	return encodeToBytes(this.IsLockedKey(string(input[1])))
}

func (this *SyncMapServerConn) lockKeysDirect(keys []string) {
	// キーはソート済みを想定
	this.lockedKeys = keys
	for _, key := range keys {
		m, ok := this.server.mutexMap.Load(key)
		if !ok {
			this.registLockDirectWhenItWontExists(key)
			m, ok = this.server.mutexMap.Load(key)
			if !ok {
				log.Panic("存在しないキーへのロックに失敗しました")
			}
		}
		m.(*sync.Mutex).Lock()
		this.server.lockedMap.Store(key, true)
	}
}

func (this *SyncMapServerConn) unlockKeysDirect(keys []string) {
	// キーはソート済みを想定
	for i := len(keys) - 1; i >= 0; i-- {
		key := keys[i]
		m, ok := this.server.mutexMap.Load(key)
		if !ok {
			log.Panic("存在しないキー" + key + "へのアンロックが掛かりました")
		}
		this.server.lockedMap.Store(key, false)
		m.(*sync.Mutex).Unlock()
	}
	this.lockedKeys = []string{}
}
func (this *SyncMapServerConn) Transaction(key string, f func(tx KeyValueStoreConn)) (isok bool) {
	return this.TransactionWithKeys([]string{key}, f)
}
func (this *SyncMapServerConn) TransactionWithKeys(keysBase []string, f func(tx KeyValueStoreConn)) (isok bool) {
	keys := keysBase
	if len(keys) > 1 { // デッドロックを防ぐためにソートしておく
		keys = make([]string, len(keysBase))
		copy(keys, keysBase)
		sort.Sort(sort.StringSlice(keys))
	}
	newConn := this.New()
	if this.IsMasterServer() {
		// サーバー側はそのまま
		newConn.lockKeysDirect(keys)
		f(newConn)
		newConn.unlockKeysDirect(keys)
	} else {
		keysEncoded := joinStrsToBytes(keys)
		newConn.send(syncMapCommandLockKey, keysEncoded)
		f(newConn)
		newConn.send(syncMapCommandUnlockKey, keysEncoded)
	}
	return true
}
func (this *SyncMapServerConn) parseLockKeys(input [][]byte) {
	keys := splitBytesToStrs(input[1])
	this.lockKeysDirect(keys)
}
func (this *SyncMapServerConn) parseUnlockKeys(input [][]byte) {
	keys := splitBytesToStrs(input[1])
	this.unlockKeysDirect(keys)
}

// 自作関数を使用する時用
func DefaultSendCustomFunction(this *SyncMapServerConn, buf []byte) []byte {
	return buf // echo server
}
func (this *SyncMapServerConn) CustomFunction(f func() []byte) []byte {
	if this.IsMasterServer() {
		return this.server.MySendCustomFunction(this, f())
	} else {
		return this.send(syncMapCommandCustom, f())
	}
}
func (this *SyncMapServerConn) parseCustomFunction(input [][]byte) []byte {
	return this.CustomFunction(func() []byte { return input[1] })
}

// 全ての要素を削除する
func (this *SyncMapServerConn) FlushAll() {
	if this.IsMasterServer() {
		var syncMap sync.Map
		this.server.SyncMap = syncMap
		var mutexMap sync.Map
		this.server.mutexMap = mutexMap
		var lockedMap sync.Map
		this.server.lockedMap = lockedMap
		this.server.keyCount = 0
	} else {
		this.send(syncMapCommandFlushAll)
	}
}

//  SyncMap で使用する関数
func (this *SyncMapServer) GetConn() *SyncMapServerConn {
	return &SyncMapServerConn{
		server:              this,
		connectionPoolIndex: NoConnectionIsSelected,
	}
}
func NewSyncMapServerConn(substanceAddress string, isMaster bool) *SyncMapServerConn {
	if isMaster {
		port, _ := strconv.Atoi(strings.Split(substanceAddress, ":")[1])
		result := newMasterSyncMapServer(port)
		result.MySendCustomFunction = DefaultSendCustomFunction
		result.InitializeFunction = func() {}
		return result.GetConn()
	} else {
		result := newSlaveSyncMapServer(substanceAddress)
		result.MySendCustomFunction = DefaultSendCustomFunction
		result.InitializeFunction = func() {}
		return result.GetConn()
	}
}
func (this *SyncMapServerConn) New() *SyncMapServerConn {
	return &SyncMapServerConn{
		server:              this.server,
		connectionPoolIndex: NoConnectionIsSelected,
	}
}

func (this SyncMapServer) IsMasterServer() bool {
	return len(this.substanceAddress) == 0
}
func (this *SyncMapServerConn) IsMasterServer() bool {
	return this.server.IsMasterServer()
}
func (this *SyncMapServerConn) IsNowTransaction() bool {
	return len(this.lockedKeys) > 0
}
func newMasterSyncMapServer(port int) *SyncMapServer {
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
		serverConn := this.GetConn()
		for {
			conn, err := listen.AcceptTCP()
			if err != nil {
				fmt.Println("Server:", err)
				continue
			}
			go func() {
				for {
					read := readAll(conn)
					interpreted := serverConn.interpretWrapFunction(read)
					writeAll(conn, interpreted)
				}
				// PoolするのでconnectionはCloseさせない。
				// conn.Close()
			}()
		}
	}()
	// 起動終了までちょっと時間がかかるかもしれないので待機しておく
	time.Sleep(10 * time.Millisecond)
	// 何も設定しなければecho
	this.MySendCustomFunction = DefaultSendCustomFunction
	// バックアップファイルが見つかればそれを読み込む
	this.readFile(this.getDefaultPath())
	// バックアッププロセスを開始する
	this.startBackUpProcess()
	return &this
}
func newSlaveSyncMapServer(substanceAddress string) *SyncMapServer {
	this := SyncMapServer{}
	this.substanceAddress = substanceAddress
	port, err := strconv.Atoi(strings.Split(substanceAddress, ":")[1])
	if err != nil {
		panic(err)
	}
	this.masterPort = port
	this.MySendCustomFunction = DefaultSendCustomFunction
	this.connectionPool = make([]*net.TCPConn, maxSyncMapServerConnectionNum)
	this.connectionPoolStatus = make([]int, maxSyncMapServerConnectionNum)
	this.connectionPoolEmptyChannel = make(chan int, maxSyncMapServerConnectionNum)
	for i := 0; i < maxSyncMapServerConnectionNum; i++ {
		this.connectionPoolEmptyChannel <- i
	}
	// 要求があって初めて接続する。再起動試験では起動順序が一律ではないため。
	// Redisがそういう仕組みなのでこちらもそのようにしておく
	return &this
}
func (this *SyncMapServer) getDefaultPath() string {
	return SyncMapBackUpPath + strconv.Itoa(this.masterPort) + ".sm"
}
func (this *SyncMapServer) writeFile(path string) {
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
	file, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	file.Write(encodeToBytes(result))
}
func (this *SyncMapServer) readFile(path string) error {
	if !this.IsMasterServer() {
		return nil
	}
	// Lock ?
	encoded, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	// 読み込めなければデータはそのまま
	conn := this.GetConn()
	conn.FlushAll()
	var decoded [][][]byte
	decodeFromBytes(encoded, &decoded)
	for _, here := range decoded {
		key := string(here[0])
		t := string(here[1])
		if strings.Compare(t, "1") == 0 {
			conn.storeDirect(key, here[2])
		} else if strings.Compare(t, "2") == 0 {
			conn.storeDirect(key, here[2:])
		} else {
			panic(nil)
		}
	}
	return nil
}
func (this *SyncMapServer) startBackUpProcess() {
	go func() {
		time.Sleep(time.Duration(DefaultBackUpTimeSecond) * time.Second)
		this.writeFile(this.getDefaultPath())
	}()
}

// 初期化データがあればそれをロード。なければ初期化の方法を書く
func (this *SyncMapServerConn) Initialize() {
	log.Println("INIT 1:", this.DBSize())
	if this.IsMasterServer() {
		path := InitMarkPath + strconv.Itoa(this.server.masterPort) + ".sm"
		log.Println("INIT 2:", this.DBSize())
		err := this.server.readFile(path)
		log.Println("INIT 3:", this.DBSize())
		if err == nil { // 読み込めたので何もしない
			return
		}
		log.Println("INIT 4:", this.DBSize())
		this.FlushAll()
		log.Println("INIT 5:", this.DBSize())
		this.server.InitializeFunction()
		log.Println("INIT 6:", this.DBSize())
		this.server.writeFile(path)
		log.Println("INIT 7:", this.DBSize())
	} else {
		this.send(syncMapCommandInitialize)
	}
	log.Println("INITIALIZZED:", this.DBSize())
}

// 自身の SyncMapからLoad / 変更できるようにpointer型で受け取ること
func (this *SyncMapServerConn) loadDirectWithDecoding(key string, res interface{}) bool {
	value, ok := this.loadDirect(key)
	if ok {
		decodeFromBytes(value.([]byte), res)
	}
	return ok
}
func (this *SyncMapServerConn) storeDirectWithEncoding(key string, value interface{}) {
	encoded := encodeToBytes(value)
	this.storeDirect(key, encoded)
}

// 集約させておくことで後で便利にする
func (this *SyncMapServerConn) loadDirect(key string) (interface{}, bool) {
	x, ok := this.server.SyncMap.Load(key)
	return x, ok
}

// 自身の SyncMapにStore. value は []byte か [][]byte 型
func (this *SyncMapServerConn) storeDirect(key string, value interface{}) {
	_, exists := this.server.SyncMap.Load(key)
	if !exists {
		this.registLockDirectWhenItWontExists(key)
		atomic.AddInt32(&this.server.keyCount, 1)
	}
	this.server.SyncMap.Store(key, value)
}
func (this *SyncMapServerConn) deleteDirect(key string) {
	_, exists := this.server.SyncMap.Load(key)
	if !exists {
		return
	}
	this.server.SyncMap.Delete(key)
	this.server.mutexMap.Delete(key)
	this.server.lockedMap.Delete(key)
	atomic.AddInt32(&this.server.keyCount, -1)
}

// キーが存在しない可能性が高い時にキーを追加する
func (this *SyncMapServerConn) registLockDirectWhenItWontExists(key string) {
	var m sync.Mutex
	_, loaded := this.server.mutexMap.LoadOrStore(key, &m)
	if !loaded {
		this.server.lockedMap.Store(key, false)
	}
}

// IsLocked とは違って自身がそれをロックしているかどうかを調べる
func (this *SyncMapServerConn) myConnectionIsLocking(key string) bool {
	if !this.IsNowTransaction() {
		return false
	}
	for _, k := range this.lockedKeys {
		if k == key {
			return true
		}
	}
	return false
}

// 生のbyteを送信
func (this *SyncMapServerConn) send(command string, packet ...[]byte) []byte {
	encoded := make([][]byte, 1+len(packet))
	encoded[0] = []byte(command)
	for i, p := range packet {
		encoded[i+1] = p
	}
	packed := join(encoded)
	if this.IsMasterServer() {
		log.Panic("Error Execute Directry On Master Server !!")
		return this.interpretWrapFunction(packed)
	} else {
		return this.sendBySlave(command, packed, packet...)
	}
}

// トランザクション
func (this *SyncMapServerConn) sendBySlave(command string, packet []byte, rawPacket ...[]byte) []byte {
	poolIndex := this.connectionPoolIndex
	if poolIndex == NoConnectionIsSelected {
		poolIndex = <-this.server.connectionPoolEmptyChannel
	}
	poolStatus := this.server.connectionPoolStatus[poolIndex]
	conn := this.server.connectionPool[poolIndex]
	if poolStatus == ConnectionPoolStatusDisconnected {
		tcpAddr, err := net.ResolveTCPAddr("tcp4", this.server.substanceAddress)
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
			this.server.connectionPoolStatus[poolIndex] = ConnectionPoolStatusDisconnected
			return this.sendBySlave(command, packet)
		}
		// NOTE: できるなら永遠に接続したい
		newConn.SetKeepAlive(true)
		// newConn.SetReadBuffer(65536)
		// newConn.SetWriteBuffer(65536)
		conn = newConn
	}
	writeAll(conn, packet)
	result := readAll(conn)
	this.server.connectionPool[poolIndex] = conn
	this.server.connectionPoolStatus[poolIndex] = ConnectionPoolStatusEmpty
	if command == syncMapCommandLockKey {
		// ロック開始 => conn に connectionPoolIndex を設定
		this.lockedKeys = splitBytesToStrs(rawPacket[0])
		this.connectionPoolIndex = poolIndex
		return result
	} else if command == syncMapCommandUnlockKey {
		// ロック終了 => conn の connectionPoolIndex を空に設定
		this.connectionPoolIndex = NoConnectionIsSelected
		this.server.connectionPoolEmptyChannel <- poolIndex
		this.lockedKeys = []string{}
		return result
	} else if len(this.lockedKeys) > 0 {
		// ロック中は他の人にあげない
		return result
	} else {
		this.server.connectionPoolEmptyChannel <- poolIndex
		return result
	}
}
