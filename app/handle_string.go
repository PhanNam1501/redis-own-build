package main

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

func handleSet(conn net.Conn, res []string) {
	expireAt := int64(0)
	if len(res) == 5 && res[3] == "PX" {
		expireMs, err := strconv.ParseInt(res[4], 10, 64)
		if err != nil {
			conn.Write([]byte("-ERR invalid expire time\r\n"))
			return
		}
		expireAt = time.Now().UnixMilli() + expireMs
	}
	mu.Lock()
	redisMap[res[1]] = &RedisValue{
		Value:    res[2],
		ExpireAt: expireAt,
	}
	mu.Unlock()
	conn.Write([]byte("+OK\r\n"))
}

func handleGet(conn net.Conn, res []string) {
	mu.RLock()
	defer mu.RUnlock()
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

func handleEcho(conn net.Conn, res []string) {
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(res[1]), res[1])
	conn.Write([]byte(response))
}

func handlePing(conn net.Conn) {
	conn.Write([]byte("+PONG\r\n"))
}
