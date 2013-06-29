// worker
package main

import (
	"bufio"
	"flag"
	"github.com/shxsun/beelog"
	"net"
	"time"
)

var (
	Flag = flag.String("flag", "0", "a flag to indentify worker")
	Addr = flag.String("addr", "localhost:7777", "address to connect")
)

func main() {
	flag.Parse()

	beelog.Info("start worker ", *Flag)
	conn, err := net.DialTimeout("tcp", *Addr, time.Second*2)
	if err != nil {
		beelog.Error("dial error:", err)
		return
	}
	defer conn.Close()

	conn.Write([]byte(*Flag + "\n"))

	// wait push message
	bf := bufio.NewReader(conn)
	for {
		line, _, err := bf.ReadLine()
		if err != nil {
			beelog.Error("readline error:", err)
			return
		}
		beelog.Info("readline:", string(line))
	}
}
