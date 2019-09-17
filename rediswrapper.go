package main

// もしものときに Redis を使いたくなった場合にでも速やかに移行できる Redisラッパー

import (
	"bytes"
	"encoding/gob"
	"log"

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
func encodeToBytes(x interface{}) []byte {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(x)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// 変更できるようにpointer型で受け取ること
func decodeFromBytes(bytes_ []byte, x interface{}) {
	var buf bytes.Buffer
	buf.Write(bytes_)
	dec := gob.NewDecoder(&buf)
	err := dec.Decode(x)
	if err != nil {
		log.Panic(err)
	}
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

func (this RedisWrapper) IncrBy(key string, value int) int {
	return int(this.Redis.IncrBy(key, int64(value)).Val())
}

// Val()
func (this RedisWrapper) FlushAll() {
	this.Redis.FlushAll()
}
