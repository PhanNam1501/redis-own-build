package main

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

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
			streamExists, _ := streamMap.CheckExist(res[1])
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
			keyStream := res[1]
			id := res[2]
			values := make(map[string]string)
			for i := 3; i+1 < len(res); i += 2 {
				values[res[i]] = res[i+1]
			}
			addedId, err := streamMap.Add(keyStream, id, values)
			mu.Unlock()
			var response string
			if err != nil {
				response = fmt.Sprintf("-%s\r\n", err.Error())
				conn.Write([]byte(response))
			} else {
				response = fmt.Sprintf("$%d\r\n%s\r\n", len(addedId), addedId)
				conn.Write([]byte(response))
			}
		case res[0] == "XRANGE" && len(res) >= 4:
			mu.RLock()
			key := res[1]
			startId := res[2]
			endId := res[3]
			entries, _ := streamMap.Range(key, startId, endId)
			mu.RUnlock()

			response := fmt.Sprintf("*%d\r\n", len(entries))
			for _, entry := range entries {
				response += "*2\r\n"
				response += fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID), entry.ID)

				kvCount := len(entry.KeyOrder) * 2
				response += fmt.Sprintf("*%d\r\n", kvCount)
				for _, k := range entry.KeyOrder {
					v := entry.Values[k]
					response += fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
					response += fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
				}
			}
			conn.Write([]byte(response))
		case res[0] == "XREAD" && len(res) >= 4 && res[1] == "streams":
			mu.RLock()
			numStreams := (len(res) - 2) / 2

			var streamResults []struct {
				key     string
				entries []interface{}
			}

			for i := 0; i < numStreams; i++ {
				keyIdx := 2 + i
				idIdx := 2 + numStreams + i
				key := res[keyIdx]
				id := res[idIdx]

				entries, _ := streamMap.ReadGreater(key, id)

				entriesArray := make([]interface{}, len(entries))
				for j, entry := range entries {
					entryArray := []interface{}{
						entry.ID,
					}
					kvArray := make([]interface{}, 0)
					for _, k := range entry.KeyOrder {
						kvArray = append(kvArray, k, entry.Values[k])
					}
					entryArray = append(entryArray, kvArray)
					entriesArray[j] = entryArray
				}

				streamResults = append(streamResults, struct {
					key     string
					entries []interface{}
				}{key, entriesArray})
			}
			mu.RUnlock()

			response := fmt.Sprintf("*%d\r\n", len(streamResults))
			for _, stream := range streamResults {
				response += "*2\r\n"
				response += fmt.Sprintf("$%d\r\n%s\r\n", len(stream.key), stream.key)
				response += fmt.Sprintf("*%d\r\n", len(stream.entries))

				for _, entry := range stream.entries {
					entryArray := entry.([]interface{})
					response += "*2\r\n"
					id := entryArray[0].(string)
					response += fmt.Sprintf("$%d\r\n%s\r\n", len(id), id)
					kvArray := entryArray[1].([]interface{})
					response += fmt.Sprintf("*%d\r\n", len(kvArray))
					for _, kv := range kvArray {
						kvStr := kv.(string)
						response += fmt.Sprintf("$%d\r\n%s\r\n", len(kvStr), kvStr)
					}
				}
			}
			conn.Write([]byte(response))

		}
	}
}
