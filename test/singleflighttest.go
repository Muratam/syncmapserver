package main

import (
	"fmt"
	"sync"

	"golang.org/x/sync/singleflight"
)

var flight singleflight.Group

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

func hoge(x int) {
	// strconv.FormatInt(x/10)
	flight.Do("a", func() (interface{}, error) {
		fmt.Println(x, "hoge")
		return x, nil
	})
}

func main() {
	times(100, func(x int) {
		// time.Sleep(time.Duration(x) * time.Nanosecond)
		hoge(x)
	})
}
