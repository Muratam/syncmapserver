package main

// もしものときに Redis を使いたくなった場合にでも速やかに移行できる Redisラッパー
// どうせシリアライズする必要があるので、 int 値以外は全て[]byteにしている。
//  (int値は IncrByの都合上そのまま置く必要があるので[]byteにしていない)
import (
	"fmt"
	"log"
	"strconv"

	"github.com/go-redis/redis"
)

type WithInitializeFunciton struct {
	InitializeFunction func()
}

type RedisWrapper struct {
	Redis        *redis.Client
	tx           *redis.Tx
	pipe         *redis.Pipeliner
	isAlreadySet bool // Transaction時に既に変更を加えるコマンドを行ったか
	server       WithInitializeFunciton
}

func NewRedisWrapper(address string, dbNumber int) *RedisWrapper {
	return &RedisWrapper{
		Redis: redis.NewClient(&redis.Options{
			Addr:     address + ":6379",
			Password: "",       // no password set
			DB:       dbNumber, // use default DB
		}),
		tx:           nil,
		pipe:         nil,
		isAlreadySet: false,
		server:       WithInitializeFunciton{func() {}},
	}
}
func (this *RedisWrapper) New() *RedisWrapper {
	return &RedisWrapper{
		Redis:        this.Redis,
		tx:           nil,
		pipe:         nil,
		isAlreadySet: false,
		server:       WithInitializeFunciton{func() {}},
	}
}
func (this *RedisWrapper) IsTransactionNow() bool {
	return this.tx != nil && this.pipe != nil
}
func (this *RedisWrapper) CheckNotSet() {
	if !this.IsTransactionNow() {
		return
	}
	if this.isAlreadySet {
		log.Panic("値を変更するコマンドが既に実行されています")
	}
}
func (this *RedisWrapper) SetSet() {
	if !this.IsTransactionNow() {
		return
	}
	this.isAlreadySet = true
}

// General Commands
func (this *RedisWrapper) Get(key string, value interface{}) bool {
	this.CheckNotSet()
	var got *redis.StringCmd
	if this.IsTransactionNow() {
		got = this.tx.Get(key)
	} else {
		got = this.Redis.Get(key)
	}
	if gotInt, err := got.Int(); err == nil {
		(*value.(*int)) = gotInt
		return true
	}
	bs, err := got.Bytes()
	if err != nil {
		return false
	}
	decodeFromBytes(bs, value)
	return true
}
func (this *RedisWrapper) Set(key string, value interface{}) {
	this.SetSet()
	if _, ok := value.(int); !ok {
		value = encodeToBytes(value)
	}
	var err error
	if this.IsTransactionNow() {
		err = (*this.pipe).Set(key, value, 0).Err()
	} else {
		err = this.Redis.Set(key, value, 0).Err()
	}
	if err != nil {
		fmt.Println("Redis Error", err)
	}
}
func (this *RedisWrapper) MGet(keys []string) MGetResult {
	this.CheckNotSet()
	var loads []interface{}
	if this.IsTransactionNow() {
		loads = this.tx.MGet(keys...).Val()
	} else {
		loads = this.Redis.MGet(keys...).Val()
	}
	result := newMGetResult()
	for i, load := range loads {
		if load == nil { // キーが存在しない
			continue
		}
		// WARN: 偶然にもEncodedな結果が数値だったときにintチェックが通ってしまうが、仕方がない...
		// IncrBy が無ければここは消してしまってもよい。
		loadedStr := load.(string)
		if valueInt, err := strconv.Atoi(loadedStr); err == nil {
			result.resultMap[keys[i]] = encodeToBytes(valueInt)
			continue
		}
		result.resultMap[keys[i]] = []byte(loadedStr)
	}
	return result
}
func (this *RedisWrapper) MSet(store map[string]interface{}) {
	this.SetSet()
	var pairs []interface{}
	for key, value := range store {
		if valueInt, ok := value.(int); ok {
			pairs = append(pairs, key, valueInt)
			continue
		}
		bs := encodeToBytes(value)
		pairs = append(pairs, key, bs)
	}
	if this.IsTransactionNow() {
		(*this.pipe).MSet(pairs...)
	} else {
		this.Redis.MSet(pairs...)
	}
}
func (this *RedisWrapper) Exists(key string) bool {
	this.CheckNotSet()
	if this.IsTransactionNow() {
		return this.tx.Exists(key).Val() == 1
	} else {
		return this.Redis.Exists(key).Val() == 1
	}
}
func (this *RedisWrapper) Del(key string) {
	this.SetSet()
	if this.IsTransactionNow() {
		(*this.pipe).Del(key)
	} else {
		this.Redis.Del(key)
	}
}
func (this *RedisWrapper) IncrBy(key string, value int) int {
	this.SetSet()
	if this.IsTransactionNow() {
		return int((*this.pipe).IncrBy(key, int64(value)).Val())
	} else {
		return int(this.Redis.IncrBy(key, int64(value)).Val())
	}
}
func (this *RedisWrapper) DBSize() int {
	this.CheckNotSet()
	if this.IsTransactionNow() {
		return int(this.tx.DBSize().Val())
	} else {
		return int(this.Redis.DBSize().Val())
	}
}
func (this *RedisWrapper) AllKeys() []string {
	this.CheckNotSet()
	if this.IsTransactionNow() {
		return this.tx.Keys("*").Val()
	} else {
		return this.Redis.Keys("*").Val()
	}
}

func (this *RedisWrapper) FlushAll() {
	this.SetSet()
	if this.IsTransactionNow() {
		(*this.pipe).FlushAll()
	} else {
		this.Redis.FlushAll()
	}
}

// List 系は全て Encode して保存(intも)
func (this *RedisWrapper) RPush(key string, values ...interface{}) int {
	this.SetSet()
	encodeds := make([]interface{}, len(values))
	for i, value := range values {
		encodeds[i] = encodeToBytes(value)
	}
	var size int64
	if this.IsTransactionNow() {
		size = (*this.pipe).RPush(key, encodeds...).Val()
	} else {
		size = this.Redis.RPush(key, encodeds...).Val()
	}
	return int(size) - 1
}

// LPop
func (this *RedisWrapper) LPop(key string, value interface{}) bool {
	this.SetSet()
	var res *redis.StringCmd
	if this.IsTransactionNow() {
		res = (*this.pipe).LPop(key)
	} else {
		res = this.Redis.LPop(key)
	}
	loads, err := res.Result()
	if err != nil {
		return false
	}
	decodeFromBytes([]byte(loads), value)
	return true
}

// RPop
func (this *RedisWrapper) RPop(key string, value interface{}) bool {
	this.SetSet()
	var res *redis.StringCmd
	if this.IsTransactionNow() {
		res = (*this.pipe).RPop(key)
	} else {
		res = this.Redis.RPop(key)
	}
	loads, err := res.Result()
	if err != nil {
		return false
	}
	decodeFromBytes([]byte(loads), value)
	return true
}
func (this *RedisWrapper) LLen(key string) int {
	this.CheckNotSet()
	var size int64
	if this.IsTransactionNow() {
		size = this.tx.LLen(key).Val()
	} else {
		size = this.Redis.LLen(key).Val()
	}
	return int(size)
}
func (this *RedisWrapper) LIndex(key string, index int, value interface{}) bool {
	this.CheckNotSet()
	var res *redis.StringCmd
	if this.IsTransactionNow() {
		res = this.tx.LIndex(key, int64(index))
	} else {
		res = this.Redis.LIndex(key, int64(index))
	}
	loads, err := res.Result()
	if err != nil {
		return false
	}
	decodeFromBytes([]byte(loads), value)
	return true
}
func (this *RedisWrapper) LSet(key string, index int, value interface{}) {
	this.SetSet()
	if this.IsTransactionNow() {
		(*this.pipe).LSet(key, int64(index), encodeToBytes(value))
	} else {
		this.Redis.LSet(key, int64(index), encodeToBytes(value))
	}
}
func (this *RedisWrapper) LRange(key string, startIndex, stopIncludingIndex int) LRangeResult {
	this.CheckNotSet()
	var strResult []string
	if this.IsTransactionNow() {
		strResult = this.tx.LRange(key, int64(startIndex), int64(stopIncludingIndex)).Val()
	} else {
		strResult = this.Redis.LRange(key, int64(startIndex), int64(stopIncludingIndex)).Val()
	}
	result := make([][]byte, len(strResult))
	for i := 0; i < len(result); i++ {
		result[i] = []byte(strResult[i])
	}
	return NewLRangeResult(result)
}
func (this *RedisWrapper) Transaction(key string, f func(tx KeyValueStoreConn)) (isok bool) {
	return this.TransactionWithKeys([]string{key}, f)
}
func (this *RedisWrapper) TransactionWithKeys(keys []string, f func(tx KeyValueStoreConn)) (isok bool) {
	if this.IsTransactionNow() {
		log.Panic("Transaction in Transacion Error")
	}
	err := this.Redis.Watch(func(tx *redis.Tx) error {
		_, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
			conn := this.New()
			conn.tx = tx
			conn.pipe = &pipe
			f(conn)
			return nil
		})
		return err
	}, keys...)
	return err == nil
}
func (this *RedisWrapper) Initialize() {
	this.FlushAll()
	this.server.InitializeFunction()
}
