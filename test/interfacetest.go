package main

import (
	"fmt"
	"sync"
)

var syncmap sync.Map

func Load(key string) (interface{}, bool) {
	val, ok := syncmap.Load(key)
	if !ok {
		return []byte(""), false
	}
	return val.([]byte), true
}
func Store(key string, value []byte) {
	syncmap.Store(key, value)
}

func main() {
	func() {
		key := "count"
		syncmap.Store(key, 1)
		x, _ := syncmap.Load(key)
		xi := x.(int)
		fmt.Println(xi)
	}()
	func() {
		key := "str"
		syncmap.Store(key, "aiueo")
		x, _ := syncmap.Load(key)
		xi := x.(string)
		fmt.Println(xi)
	}()
	func() {
		key := "bytes"
		syncmap.Store(key, []byte("aiueo"))
		x, _ := syncmap.Load(key)
		xi := x.([]byte)
		fmt.Println(xi)
	}()
	func() {
		key := "count"
		x, _ := syncmap.Load(key)
		xi := x.(int)
		fmt.Println(xi)
	}()
	func() {
		key := "str"
		x, _ := syncmap.Load(key)
		xi := x.(string)
		fmt.Println(xi)
	}()
	func() {
		key := "bytes"
		x, _ := syncmap.Load(key)
		xi := x.([]byte)
		fmt.Println(xi)
	}()

}
