package main

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	// "github.com/jmoiron/sqlx"
)

const OutputFieldName = false // User{ID:1,Name:"aa"} にするか User{1,"aa"} にするか

func main() {
	// Goのコードそのままの形で出力してくれるOutput関数のサンプル
	tryUserSample(100)
	// "github.com/jmoiron/sqlx" か "database/sql" かはコード次第
}
func tryUserFromDBSQLX() {
	/*
		// 書きやすい
		// "github.com/jmoiron/sqlx"
		dsn := fmt.Sprintf( // コードをそのまま持ってくること
			"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=true&loc=Local&interpolateParams=true",
			"user",     // "isu.."
			"password", // "isu.."
			"host",     // "127.0.0.1"
			"port",     // "3306"
			"dbname",   // "isu.."
		)
		dbx, err := sqlx.Open("mysql", dsn)
		if err != nil {
			panic(err)
		}
		defer dbx.Close()
		type User struct { // 最初は大文字にすること
			ID           int64      `json:"id" db:"id"`
			AccountName  string     `json:"account_name" db:"account_name"`
			Address      string     `json:"address,omitempty" db:"address"`
			NumSellItems int        `json:"num_sell_items" db:"num_sell_items"`
			LastBump     time.Time  `json:"-" db:"last_bump"`
			CreatedAt    *time.Time `json:"-" db:"created_at"`
			Children     []User
		}
		users := []User{}
		err = dbx.Select(&users, "SELECT * FROM `users`")
		if err != nil {
			panic(err)
		}
		Output("initialUsers", users)
	*/
}

func tryUserFromDBSample() {
	//[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	db, err := sql.Open("mysql", "sample:sample@tcp(127.0.0.1:3306)/sample")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	rows, err := db.Query(`select * from users where id < ?`, 6)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	type User struct { // 最初は大文字にすること
		ID           int64      `json:"id" db:"id"`
		AccountName  string     `json:"account_name" db:"account_name"`
		Address      string     `json:"address,omitempty" db:"address"`
		NumSellItems int        `json:"num_sell_items" db:"num_sell_items"`
		LastBump     time.Time  `json:"-" db:"last_bump"`
		CreatedAt    *time.Time `json:"-" db:"created_at"`
		Children     []User
	}
	users := []User{}
	for rows.Next() {
		user := User{}
		err := rows.Scan(&user.ID, &user.AccountName) // ... 頑張って書く
		if err != nil {
			panic(err)
		}
		users = append(users, user)
	}
	Output("initialUsers", users)

}

func tryUserSample(n int) {
	// NOTE: これはUser型で書かれたテンプレートなのですきなものに置き換える
	// NOTE: ここの users は本当はDBから抜き出す.これは一例
	type User struct { // 最初は大文字にすること
		ID           int64      `json:"id" db:"id"`
		AccountName  string     `json:"account_name" db:"account_name"`
		Address      string     `json:"address,omitempty" db:"address"`
		NumSellItems int        `json:"num_sell_items" db:"num_sell_items"`
		LastBump     time.Time  `json:"-" db:"last_bump"`
		CreatedAt    *time.Time `json:"-" db:"created_at"`
		Children     []User
	}
	makeTestUser := func(i int) User {
		now := time.Now().Truncate(time.Second)
		return User{int64(i), "ac" + strconv.Itoa(i), "ad" + strconv.Itoa(i), i, now, &now, []User{}}
	}
	u := makeTestUser(1)
	u.Children = append(u.Children, makeTestUser(2)) // 配列を要素に持っていても大丈夫
	users := []User{makeTestUser(0), u}
	for i := 2; i < n; i++ {
		users = append(users, makeTestUser(i))
	}
	// NOTE: 目的に応じてソートするとよい
	// sort.Slice(users, func(i, j int) bool {
	// 	return users[i].ID < users[i].ID
	// })
	Output("initialUsers", users)
	// NOTE: 配列でも map でもどちらでも可能
	// Output("initialUsers", map[int]User{1: makeTestUser(1), 3: makeTestUser(3)})
}

func Normalize(x reflect.Type) string {
	return strings.Replace(fmt.Sprint(x), "main.", "", -1)
}

// 基本は Printf %#v でよいのだが、{,*}time.Time など,時々変なものがあるので
func PrintRecursive(v reflect.Value, indent int, uniqName string) {
	t := v.Type()
	tName := Normalize(t)
	switch t.Kind() {
	case reflect.Slice:
		if v.Len() == 0 {
			fmt.Print(tName, "{}")
		} else {
			fmt.Println(tName, "{")
			for i := 0; i < v.Len(); i++ {
				for j := 0; j <= indent; j++ {
					fmt.Print("\t")
				}
				PrintRecursive(v.Index(i), indent+1, uniqName)
				fmt.Println(",")
			}
			fmt.Print("}")
		}
	case reflect.Struct:
		switch tName {
		case "time.Time":
			fmt.Print(`timeFor`, uniqName, `("`, v, `")`)
		default: // 普通の構造体
			fmt.Print(tName, "{")
			for i := 0; i < t.NumField(); i++ {
				if OutputFieldName {
					fmt.Print(t.Field(i).Name, ":")
				}
				PrintRecursive(v.Field(i), indent+1, uniqName)
				fmt.Print(",")
			}
			fmt.Print("}")
		}
	case reflect.Ptr:
		switch tName {
		case "*time.Time":
			fmt.Print(`timeFor`, uniqName, `Ptr("`, v, `")`)
		default:
			log.Panic("\nNOT SUPPORTED PTR:", tName)
		}
	case reflect.Map:
		if v.Len() == 0 {
			fmt.Print(tName, "{}")
		} else {
			fmt.Println(tName, "{")
			keys := v.MapKeys()
			sort.Slice(keys, func(i, j int) bool {
				return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j])
			})
			for _, mk := range keys {
				mv := v.MapIndex(mk)
				for j := 0; j <= indent; j++ {
					fmt.Print("\t")
				}
				fmt.Printf("%#v:", mk)
				PrintRecursive(mv, indent+1, uniqName)
				fmt.Println()
			}
			fmt.Print("}")
		}
	default:
		fmt.Printf("%#v", v)
	}
}

func Output(targetName string, targets interface{}) {
	fmt.Print(`
package main
import "time"
func timeFor` + targetName + `(s string) time.Time {
	result, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", s)
	return result
}
func timeFor` + targetName + `Ptr(s string) *time.Time {
	result, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", s)
	return &result
}
`)
	fmt.Print("var ", targetName, " = ")
	PrintRecursive(reflect.ValueOf(targets), 0, targetName)
	fmt.Println()
}
