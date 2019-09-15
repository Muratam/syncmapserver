package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"
)

// いろいろな型を保存したい
// WARN: 大文字のものしか保存されないことに注意！！！
type A struct {
	Str      string
	Int      int
	Int64    int64
	Bytes    []byte
	Time     time.Time
	Children []A
	Mymap0   map[string]string
	Mymap2   map[string]A
	Time2    *time.Time // これはいける
	// WARN: 以下のような pointer type は変換不可能です。
	// Parent *A
	// children2 []*A
	// mymap1    map[string]*A
}

// 	H string `json:"account_name" db:"account_name"`
// 	G []byte `json:"-" db:"hashed_password"`
func testA() {
	var parent A
	parent.Str = "parent"
	var ab A
	ab.Str = "Hoge"
	ab.Int = 72
	ab.Int64 = 720
	ab.Bytes = []byte("abc")
	ab.Time = time.Now()
	// t := time.Now()
	// ab.Time2 = &t
	ab.Children = []A{parent}
	// ab.Parent = &parent
	ab.Mymap0 = map[string]string{}
	ab.Mymap0["aiu"] = "eo"
	ab.Mymap0["kaki"] = "kuke"
	ab.Mymap2 = map[string]A{}
	ab.Mymap2["aaa"] = parent
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(ab)
	if err != nil {
		panic(err)
	}
	var a A
	dec := gob.NewDecoder(&buf)
	bytes := buf.Bytes()
	err = dec.Decode(&a)
	if err != nil {
		panic(err)
	}
	fmt.Println(bytes)
	fmt.Println(ab)
	fmt.Println(a)
}

type B struct {
	T *time.Time
}

func testB() []byte {
	var b B
	t := time.Now()
	b.T = &t
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(b)
	if err != nil {
		panic(err)
	}
	var buf2 bytes.Buffer
	bytes := buf.Bytes()
	buf2.Write(bytes)
	var a B
	dec := gob.NewDecoder(&buf2)
	err = dec.Decode(&a)
	if err != nil {
		panic(err)
	}
	fmt.Println(bytes)
	fmt.Println(b)
	fmt.Println(a)
	return bytes
}
func testC(bytess []byte) {
	var buf bytes.Buffer
	buf.Write(bytess)
	bytes := bytess
	fmt.Println(bytes)
	var a B
	dec := gob.NewDecoder(&buf)
	err := dec.Decode(&a)
	if err != nil {
		panic(err)
	}
	fmt.Println(a)
}

func main() {
	// testA()
	// bytess := testB()
	// testC(bytess)
	testC([]byte{
		22, 255, 129, 3, 1, 1, 1, 66, 1, 255, 130, 0, 1, 1, 1, 1, 84, 1, 255, 132, 0, 0, 0, 10, 255, 131, 5, 1, 2, 255, 134, 0, 0, 0, 20, 255, 130, 1, 15, 1, 0, 0, 0, 14, 213, 16, 10, 0, 45, 4, 160, 176, 2, 28, 0,
	})
}
