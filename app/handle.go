package main

import (
	"fmt"
	"net"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

type RedisValue struct {
	Value    string
	ExpireAt int64 // millisecond timestamp when key expires
}

var redisMap map[string]*RedisValue

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
		switch {
		case res[0] == "PING":
			conn.Write([]byte("+PONG\r\n"))
		case res[0] == "ECHO" && len(res) > 1:
			response := fmt.Sprintf("$%d\r\n%s\r\n", len(res[1]), res[1])
			conn.Write([]byte(response))
		case res[0] == "SET" && len(res) > 2:
			expireAt := int64(0)
			if len(res) == 5 && res[3] == "PX" {
				expireMs := 0
				for _, x := range res[4] {
					expireMs = expireMs*10 + int(x-'0')
				}
				expireAt = time.Now().UnixMilli() + int64(expireMs)
			}
			redisMap[res[1]] = &RedisValue{
				Value:    res[2],
				ExpireAt: expireAt,
			}
			conn.Write([]byte("+OK\r\n"))
		case res[0] == "GET" && len(res) > 1:
			val, ok := redisMap[res[1]]
			if !ok {
				conn.Write([]byte("$-1\r\n"))
			} else if val.ExpireAt > 0 && time.Now().UnixMilli() > val.ExpireAt {
				delete(redisMap, res[1])
				conn.Write([]byte("$-1\r\n"))
			} else {
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(val.Value), val.Value)
				conn.Write([]byte(response))
			}
		}
	}
}
