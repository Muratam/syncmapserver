package main

import (
	"fmt"
	"os"
	"strconv"
)

func backup() {
	InitForBenchMGetMSetUser4000()
	smMaster.FlushAll()
	for i := 0; i < 4000; i++ {
		key := strconv.Itoa(i)
		u := randUser()
		smMaster.Set(key, u)
		smMaster.RPush("a", u)
	}
	smMaster.ForceWriteNow()
}
func readCheck() {
	for i := 0; i < 4000; i++ {
		key := strconv.Itoa(i)
		u1 := User{}
		smMaster.Get(key, &u1)
		u2 := User{}
		smMaster.LIndex("a", i, &u2)
		assert(u1 == u2)
	}
	u1 := User{}
	smMaster.LIndex("a", 0, &u1)
	fmt.Println("OK: Example::", u1)
}
func main() {
	for _, v := range os.Args {
		if v == "store" {
			backup()
			break
		}
	}
	readCheck()
}
