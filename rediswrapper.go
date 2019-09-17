package main

// もしものときに Redis を使いたくなった場合にでも速やかに移行できる Redisラッパー
// どうせシリアライズする必要があるので、 int 値以外は全て[]byteにしている。
//  (int値は IncrByの都合上そのまま置く必要があるので[]byteにしていない)
import (
	"strconv"

	"github.com/go-redis/redis"
)

// TODO: BackUp

type RedisWrapper struct {
	Redis *redis.Client
}

func NewRedisWrapper(address string) RedisWrapper {
	var result RedisWrapper
	result.Redis = redis.NewClient(&redis.Options{
		Addr:     address,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return result
}

func (this RedisWrapper) Get(key string, value interface{}) bool {
	got := this.Redis.Get(key)
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
func (this RedisWrapper) Set(key string, value interface{}) {
	if valueInt, ok := value.(int); ok {
		this.Redis.Set(key, valueInt, 0)
		return
	}
	bs := encodeToBytes(value)
	this.Redis.Set(key, bs, 0)
}

func (this RedisWrapper) MGet(keys []string) MGetResult {
	// TODO: int64
	loads := this.Redis.MGet(keys...).Val()
	result := newMGetResult()
	for i, load := range loads {
		// intならDecodingしておく
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
func (this RedisWrapper) MSet(store map[string]interface{}) {
	var pairs []interface{}
	for key, value := range store {
		if valueInt, ok := value.(int); ok {
			pairs = append(pairs, key, valueInt)
			continue
		}
		bs := encodeToBytes(value)
		pairs = append(pairs, key, bs)
	}
	this.Redis.MSet(pairs...)
}

func (this RedisWrapper) IncrBy(key string, value int) int {
	return int(this.Redis.IncrBy(key, int64(value)).Val())
}

func (this RedisWrapper) Exists(key string) bool {
	return this.Redis.Exists(key).Val() == 1
}
func (this RedisWrapper) Del(key string) {
	this.Redis.Del(key)
}
func (this RedisWrapper) DBSize() int {
	return int(this.Redis.DBSize().Val())
}

// List 系は全て Encode して保存(intも)
func (this RedisWrapper) RPush(key string, value interface{}) int {
	size := this.Redis.RPush(key, encodeToBytes(value)).Val()
	return int(size) - 1
}
func (this RedisWrapper) LLen(key string) int {
	size := this.Redis.LLen(key).Val()
	return int(size) - 1
}
func (this RedisWrapper) LIndex(key string, index int, value interface{}) bool {
	loads, err := this.Redis.LIndex(key, int64(index)).Result()
	if err != nil {
		return false
	}
	decodeFromBytes([]byte(loads), value)
	return true
}
func (this RedisWrapper) LSet(key string, index int, value interface{}) {
	this.Redis.LSet(key, int64(index), encodeToBytes(value))
}

func (this RedisWrapper) FlushAll() {
	this.Redis.FlushAll()
}
