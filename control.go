// control.go
/*
Redis store data description
	worker:hset		// HSET work 列表
	todo:list		// LIST 任务队列
	job:count		// STRING 任务数
	job:info:id		// STRING 任务详细信息
	job:status:id	// HSET 各个work的完成状态

工作流程
1. 存储：通过job:count的INCR操作获取任务id，任务具体的信息存储在job:info:id中
2. 入队：将id使用RPUSH插入到todo:list列表中。
3. 查询：通过查询job:status:id，获取对应任务的完成信息。
*/
package main

import (
	//"bufio"
	"flag"
	"fmt"
	"github.com/shxsun/beelog"
	"github.com/shxsun/redis"
	//"io"
	"strconv"
)

var (
	client  = redis.Client{}
	Message = flag.String("m", "hello world", "message send to worker")
	Status  = flag.String("status", "-", "specify status id to check result")
)

func init() {
	client.Addr = "127.0.0.1:6379"
	client.Password = ""
}

func main() {
	flag.Parse()
	if *Status != "-" {
		keys, err := client.Hkeys("job:status:" + *Status)
		if err != nil {
			beelog.Error(err)
			return
		}
		for _, key := range keys {
			val := redis.MustString(client.Hget("job:status:"+*Status, key))
			fmt.Println("- ", key, val)
		}
	} else {
		id, err := client.Incr("job:count")
		if err != nil {
			beelog.Error(err)
			return
		}
		fmt.Println("ID: ", id)

		keyInfo := "job:info:" + strconv.Itoa(int(id))
		err = client.Set(keyInfo, []byte(*Message))
		if err != nil {
			beelog.Error(err)
			return
		}

		keyTodo := "todo:list"
		err = client.Rpush(keyTodo, []byte(strconv.Itoa(int(id))))
		if err != nil {
			beelog.Error(err)
			return
		}
	}
}
