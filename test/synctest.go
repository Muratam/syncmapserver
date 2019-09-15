package main

import (
	"fmt"
	"sync"

	"github.com/go-redis/redis"
)

const max = 500

func times(x int, f func(x int)) {
	var wg sync.WaitGroup
	for i := 0; i < x; i++ {
		wg.Add(1)
		j := i
		go func() {
			defer wg.Done()
			f(j)
		}()
	}
	wg.Wait()
}

func series(x int, f func(x int)) {
	for i := 0; i < x; i++ {
		f(i)
	}
}

func naive() {
	count := 0
	times(max, func(x int) {
		y := count
		count = y + 1
	})
	fmt.Println(count, "?=", max)
}

func syncNaive() {
	mutex := new(sync.Mutex)
	count := 0
	times(max, func(x int) {
		mutex.Lock()
		y := count
		count = y + 1
		mutex.Unlock()
	})
	fmt.Println(count, "?=", max)
}

var err error

func redisIncl() {
	// トランザクションしたいね(複数のclientでできればよさそう)
	// init
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	client.Set("key", 0, 0)
	// TCPを経由しない実装に比べると雲泥の差
	// (全て直列にやるのに比べて更に)2~3倍遅いよ〜
	// 楽観ロックだしなー
	times(max, func(x int) {
		for {
			count := 0
			err := client.Watch(func(tx *redis.Tx) error {
				_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
					count, err = tx.Get("key").Int()
					pipe.Set("key", count+1, 0)
					return nil
				})
				return err
			}, "key")
			if err == nil {
				fmt.Println(count)
				break
			}
		}
	})
	val, _ := client.Get("key").Int()
	fmt.Println("count", val)
}
func syncMapServerIncl() {
	address := "127.0.0.1:8888"
	var masterSyncMapServer = NewMasterOrSlaveSyncMapServer(address, true, DefaultSendFunction)
	// var slaveSyncMapServer = NewMasterOrSlaveSyncMapServer(address, false, DefaultSendFunction)
	masterSyncMapServer.Store("x", 0)
	times(max, func(x int) {
		masterSyncMapServer.StartTransaction(func(tx *SyncMapServerTransaction) {
			x := 0
			tx.Load("x", &x)
			x += 1
			tx.Store("x", x)
			fmt.Println(x)
		})
	})
	val := 0
	masterSyncMapServer.Load("x", &val)
	fmt.Println("count", val)
}

func main() {
	// naive()
	// syncNaive()
	// redisIncl()
	syncMapServerIncl()
}
