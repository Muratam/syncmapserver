package main

import (
	"fmt"
	"os"

	"github.com/go-redis/redis"
)

// TODO: BackUp
const MasterServerAddressWhenNO_REDIS_HOST = "12.34.56.78"

func parseRedisHostIP() string {
	result := os.Getenv("REDIS_HOST")
	if result == "" {
		return MasterServerAddressWhenNO_REDIS_HOST
	}
	return result
}

type RedisWrapper struct {
}

func NewRedisWrapper() RedisWrapper {
	var result RedisWrapper
	return result
}

func Example() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
	err = client.Set("key", "value", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := client.Get("key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	val2, err := client.Get("key2").Result()
	if err == redis.Nil {
		fmt.Println("key2 does not exist")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("key2", val2)
	}
	// Output: key value
	// key2 does not exist
}
func main() {
	Example()
}
