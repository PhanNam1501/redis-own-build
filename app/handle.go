package main

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/queue"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

type RedisValue struct {
	Value    string
	ExpireAt int64
}

type Stream struct {
	Entries []Entry
}

type Entry struct {
	ID     string
	Values map[string]string
}

var redisMap map[string]*RedisValue
var mu sync.RWMutex
var listMap queue.Queue
var streams map[string]*Stream

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
				expireMs, err := strconv.ParseInt(res[4], 10, 64)
				if err != nil {
					conn.Write([]byte("-ERR invalid expire time\r\n"))
					continue
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
		case res[0] == "GET" && len(res) > 1:
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
		case res[0] == "RPUSH" && len(res) > 2:
			mu.Lock()
			length := listMap.RPush(res[1], res[2:]...)
			mu.Unlock()

			response := fmt.Sprintf(":%d\r\n", length)
			conn.Write([]byte(response))
		case res[0] == "LPUSH" && len(res) > 2:
			mu.Lock()
			length := listMap.LPush(res[1], res[2:]...)
			mu.Unlock()

			response := fmt.Sprintf(":%d\r\n", length)
			conn.Write([]byte(response))
		case res[0] == "LPOP" && len(res) > 1:
			mu.Lock()
			arr := []string{}
			if len(res) == 3 {
				cnt, _ := strconv.Atoi(res[2])
				arr = listMap.LPOP(res[1], cnt)
			} else {
				arr = listMap.LPOP(res[1], 1)
			}
			mu.Unlock()
			if len(res) == 2 {
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(arr[0]), arr[0])
				conn.Write([]byte(response))
			} else {
				response := fmt.Sprintf("*%d\r\n", len(arr))
				for _, elem := range arr {
					response += fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem)
				}
				conn.Write([]byte(response))
			}
		case res[0] == "BLPOP" && len(res) == 3:
			exp, err := strconv.ParseFloat(res[2], 64)
			if err != nil {
				conn.Write([]byte("-ERR invalid timeout\r\n"))
				continue
			}
			arr := listMap.BLPOP(res[1], exp)
			if len(arr) == 0 {
				conn.Write([]byte("*-1\r\n"))
			} else {
				response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(res[1]), res[1], len(arr[0]), arr[0])
				conn.Write([]byte(response))
			}
		case res[0] == "LRANGE" && len(res) > 2:
			mu.RLock()
			start, _ := strconv.Atoi(res[2])
			end, _ := strconv.Atoi(res[3])
			elements := listMap.Query(res[1], start, end)
			mu.RUnlock()

			response := fmt.Sprintf("*%d\r\n", len(elements))
			for _, elem := range elements {
				response += fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem)
			}
			conn.Write([]byte(response))
		case res[0] == "LLEN" && len(res) == 2:
			mu.RLock()
			res := listMap.Len(res[1])
			response := fmt.Sprintf(":%d\r\n", res)
			conn.Write([]byte(response))
		case res[0] == "TYPE" && len(res) == 2:
			mu.RLock()
			_, stringExists := redisMap[res[1]]
			_, listExists := listMap.CheckExist(res[1])
			_, streamExists := streams[res[1]]
			mu.RUnlock()

			var response string
			if stringExists {
				response = "+string\r\n"
			} else if listExists {
				response = "+list\r\n"
			} else if streamExists {
				response = "+stream\r\n"
			} else {
				response = "+none\r\n"
			}
			conn.Write([]byte(response))
		case res[0] == "XADD" && len(res) >= 2:
			mu.Lock()
			defer mu.Unlock()
			keyStream := res[1]
			id := res[2]
			entries := []Entry{}
			for i := 3; i < len(res)-1; i += 2 {
				entry := Entry{
					ID: id,
				}
				entry.Values[res[i]] = res[i+1]
				entries = append(entries, entry)
			}
			stream := &Stream{
				Entries: entries,
			}
			streams[keyStream] = stream
			length := (len(res) - 3) / 2
			response := fmt.Sprintf("$%d\r\n%s\r\n", length, id)
			conn.Write([]byte(response))
		}
	}
}
