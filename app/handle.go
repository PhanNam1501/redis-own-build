package main

import (
	"fmt"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/handle"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

func handleConnection(conn net.Conn, h handle.IHandler) {
	for {
		r := resp.NewResp(conn)
		res := r.DecodeResp()
		if res == nil {
			break
		}

		if len(res) == 0 {
			continue
		}
		fmt.Println("DEBUG Command:", res)
		switch {
		case res[0] == "PING":
			h.Ping(conn)
		case res[0] == "ECHO" && len(res) > 1:
			h.Echo(conn, res)
		case res[0] == "SET" && len(res) > 2:
			h.Set(conn, res)
		case res[0] == "GET" && len(res) > 1:
			h.Get(conn, res)
		case res[0] == "INCR" && len(res) == 2:
			h.INCR(conn, res)
		case res[0] == "RPUSH" && len(res) > 2:
			h.RPush(conn, res)
		case res[0] == "LPUSH" && len(res) > 2:
			h.LPush(conn, res)
		case res[0] == "LPOP" && len(res) > 1:
			h.LPop(conn, res)
		case res[0] == "BLPOP" && len(res) == 3:
			h.BLPop(conn, res)
		case res[0] == "LRANGE" && len(res) > 2:
			h.LRange(conn, res)
		case res[0] == "LLEN" && len(res) == 2:
			h.LLen(conn, res)
		case res[0] == "TYPE" && len(res) == 2:
			h.Type(conn, res)
		case res[0] == "XADD" && len(res) >= 2:
			h.XAdd(conn, res)
		case res[0] == "XRANGE" && len(res) >= 4:
			h.XRange(conn, res)
		case res[0] == "XREAD" && len(res) >= 4 && strings.ToUpper(res[1]) == "STREAMS":
			h.XRead(conn, res)
		case res[0] == "XREAD" && len(res) > 5 && strings.ToUpper(res[1]) == "BLOCK" && strings.ToUpper(res[3]) == "STREAMS":
			h.XReadBlock(conn, res)
		}
	}
}
