package main

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/internal/queue"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/stream"
)

type RedisValue struct {
	Value    string
	ExpireAt int64
}

var redisMap map[string]*RedisValue
var mu sync.RWMutex
var listMap queue.Queue
var streamMap stream.Stream

func handleConnection(conn net.Conn) {
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
			handlePing(conn)
		case res[0] == "ECHO" && len(res) > 1:
			handleEcho(conn, res)
		case res[0] == "SET" && len(res) > 2:
			handleSet(conn, res)
		case res[0] == "GET" && len(res) > 1:
			handleGet(conn, res)
		case res[0] == "RPUSH" && len(res) > 2:
			handleRPush(conn, res)
		case res[0] == "LPUSH" && len(res) > 2:
			handleLPush(conn, res)
		case res[0] == "LPOP" && len(res) > 1:
			handleLPop(conn, res)
		case res[0] == "BLPOP" && len(res) == 3:
			handleBLPop(conn, res)
		case res[0] == "LRANGE" && len(res) > 2:
			handleLRange(conn, res)
		case res[0] == "LLEN" && len(res) == 2:
			handleLLen(conn, res)
		case res[0] == "TYPE" && len(res) == 2:
			handleType(conn, res)
		case res[0] == "XADD" && len(res) >= 2:
			handleXAdd(conn, res)
		case res[0] == "XRANGE" && len(res) >= 4:
			handleXRange(conn, res)
		case res[0] == "XREAD" && len(res) >= 4 && strings.ToUpper(res[1]) == "STREAMS":
			handleXRead(conn, res)
		case res[0] == "XREAD" && len(res) > 5 && strings.ToUpper(res[1]) == "BLOCK" && strings.ToUpper(res[3]) == "STREAMS":
			handleXReadBlock(conn, res)
		}
	}
}
