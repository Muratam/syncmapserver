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
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	client.Set("key", 0, 0)
	times(max, func(x int) {
		for {
			count := 0
			// [Get...] -> [Set...] しかありえない。
			err := client.Watch(func(tx *redis.Tx) error {
				// そのキーに関するものは全て Watch されている
				// ここで実行中のコマンドの「最中に」他のコマンドが割り込むことはない。

				// まずは Get のフェイズ
				count, _ = tx.Get("key").Int()

				// つづいて Set のフェイズ
				_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
					pipe.Set("key", count+1, 0)
					return nil
				})
				return err
			}, "key")
			if err == nil {
				fmt.Println(count)
				return
			} else {
				// Set は全てなかったことに
				fmt.Println("楽観ロックしっぱい")

			}
		}
	})
	val, _ := client.Get("key").Int()
	fmt.Println("count", val)
}

func pp(x string) {
	fmt.Println(x)
}

func main() {
	pp("aiueo"[2:])
	// naive()
	// syncNaive()
	// redisIncl()
}
