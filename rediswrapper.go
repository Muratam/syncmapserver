package main

// もしものときに Redis を使いたくなった場合にでも速やかに移行できる Redisラッパー
// どうせシリアライズする必要があるので、 int 値以外は全て[]byteにしている。
import (
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
		if load != nil {
			// WARN: string ??
			result.resultMap[keys[i]] = []byte(load.(string))
		}
	}
	return result
}
func (this RedisWrapper) MSet(store map[string]interface{}) {
	// TODO: int64
	var pairs []interface{}
	for k, v := range store {
		bs := encodeToBytes(v)
		pairs = append(pairs, k, bs)
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

// Val()
func (this RedisWrapper) FlushAll() {
	this.Redis.FlushAll()
}
