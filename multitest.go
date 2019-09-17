package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// MULTIGET / MULTISET のテスト

var smUserServer = NewMasterOrSlaveSyncMapServer("127.0.0.1:8888", os.Getenv("IS_MASTER") != "", DefaultSendCustomFunction)

func setSingle() {
	// SET を使って 4000データ作成
	for i := 1; i <= 4000; i++ {
		is := strconv.Itoa(i)
		smUserServer.Store(is, i)
	}
}

func getSingle() {
	// GET を使って 4000データロード
	for i := 1; i <= 4000; i++ {
		is := strconv.Itoa(i)
		x := 0
		smUserServer.Load(is, &x)
		fmt.Print(x, " ")
	}
}
func getMulti() {
	// MGETを使って4000データロード
	keys := make([]string, 0)
	for i := 1; i <= 4000; i++ {
		keys = append(keys, strconv.Itoa(i))
	}
	values := smUserServer.MultiLoad(keys)
	for i, _ := range keys {
		value := values[i]
		x := 0
		DecodeFromBytes(value, &x)
		fmt.Print(x," ")
	}
}
func setMulti(){
	keys := make([]string,0)
	values := make([][]byte,0)
	for i := 1 ; i <= 4000 ; i++{
		is := strconv.Itoa(i)
		keys = append(keys,is)
		values = append(values,EncodeToBytes(i))
	}
	smUserServer.MultiStore(keys,values)
}

func main() {
	fmt.Println("IS_MASTER:", smUserServer.IsOnThisApp())
	setSingle()
	setMulti()
	getSingle()
	getMulti()
	for {
		time.Sleep(1000 * time.Millisecond)
	}
}
