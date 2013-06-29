// master.go
/*
Redis store data description
	worker:hset		// HSET work 列表
	todo:list		// LIST 任务队列
	job:info:id		// STRING 任务详细信息
	job:status:id	// HSET 各个work的完成状态

工作流程
1. 接手worker的tcp连接，并写work id到worker:hset中
2. 从todo:list使用LPOP取任务
3. 查询：通过查询job:status:id，获取对应任务的信息。
4. 下发任务到worker，将任务执行的状态更新到job:status:id中
*/

package main

import (
	"bufio"
	"github.com/shxsun/beelog"
	"github.com/shxsun/redis"
	"net"
	"time"
)

var (
	client = redis.Client{}
	m      = make(map[string]net.Conn)
)

func Listen(lis net.Listener) {
	defer lis.Close()
	for {
		conn, err := lis.Accept()
		if err != nil {
			beelog.Warn(err)
			continue
		}
		bf := bufio.NewReader(conn)
		line, _, err := bf.ReadLine()
		if err != nil {
			beelog.Warn("readline error:", err)
			conn.Close()
		}
		beelog.Info("ReadLine: ", string(line))
		m[string(line)] = conn
	}
}

func init() {
	client.Addr = "127.0.0.1:6379"
}

func main() {
	beelog.Info("Master start ...")
	lis, err := net.Listen("tcp", "127.0.0.1:7777")
	if err != nil {
		beelog.Error(err)
		return
	}
	go Listen(lis)

	for {
		todo, err := client.Lpop("todo:list")
		if err != nil {
			beelog.Info("sleep 2s: ", err)
			time.Sleep(time.Second * 2)
			continue
		}
		id := string(todo)
		beelog.Info("todo id: ", id)
		info := redis.MustString(client.Get("job:info:" + id))
		time.Sleep(time.Second * 1)
		beelog.Debug("info:", info)
		for worker, conn := range m {
			client.Hset("job:status:"+id, worker, []byte("Done"))
			beelog.Debug("worker:", worker)
			conn.Write([]byte("Hi " + worker + " msg: " + info + "\n"))
		}
	}
}
