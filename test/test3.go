package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

func EncodeToBytes(x interface{}) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(x); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// NOTE: 変更できるようにpointer型で受け取ること
func DecodeFromBytes(bytes_ []byte, x interface{}) {
	var buf bytes.Buffer
	buf.Write(bytes_)
	dec := gob.NewDecoder(&buf)
	err := dec.Decode(x)
	if err != nil {
		panic(err)
	}
}

func main() {
	x := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	xs := make([][]byte, 0)
	xs = append(xs, x)
	xs = append(xs, x)
	xs = append(xs, x)
	fmt.Println(xs)
	y := EncodeToBytes(xs)
	fmt.Println(y)
	xs2 := make([][]byte, 0)
	DecodeFromBytes(y, &xs2)
	fmt.Println(xs2)
}
