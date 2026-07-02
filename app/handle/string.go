package handle

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

func (h *Handler) Set(conn net.Conn, res []string) {
	expireAt := int64(0)
	if len(res) == 5 && res[3] == "PX" {
		expireMs, err := strconv.ParseInt(res[4], 10, 64)
		if err != nil {
			conn.Write([]byte("-ERR invalid expire time\r\n"))
			return
		}
		expireAt = time.Now().UnixMilli() + expireMs
	}
	h.Mu.Lock()
	h.RedisMap[res[1]] = &RedisValue{
		Value:    res[2],
		ExpireAt: expireAt,
	}
	h.Mu.Unlock()
	conn.Write([]byte("+OK\r\n"))
}

func (h *Handler) Get(conn net.Conn, res []string) {
	h.Mu.RLock()
	defer h.Mu.RUnlock()
	val, ok := h.RedisMap[res[1]]
	if !ok {
		conn.Write([]byte("$-1\r\n"))
	} else if val.ExpireAt > 0 && time.Now().UnixMilli() > val.ExpireAt {
		delete(h.RedisMap, res[1])
		conn.Write([]byte("$-1\r\n"))
	} else {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(val.Value), val.Value)
		conn.Write([]byte(response))
	}
}

func (h *Handler) Echo(conn net.Conn, res []string) {
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(res[1]), res[1])
	conn.Write([]byte(response))
}

func (h *Handler) Ping(conn net.Conn) {
	conn.Write([]byte("+PONG\r\n"))
}

func (h *Handler) INCR(conn net.Conn, res []string) {
	key := res[1]
	val, ok := h.RedisMap[key]
	if !ok {
		conn.Write([]byte("$-1\r\n"))
	} else {
		valInt, _ := strconv.Atoi(val.Value)
		valInt++
		h.RedisMap[res[1]] = &RedisValue{
			Value:    strconv.Itoa(valInt),
			ExpireAt: val.ExpireAt,
		}
		response := fmt.Sprintf(":%d\r\n", valInt)
		conn.Write([]byte(response))
	}
}
