package handle

import (
	"net"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/internal/queue"
	"github.com/codecrafters-io/redis-starter-go/internal/stream"
)

type RedisValue struct {
	Value    string
	ExpireAt int64
}

type IHandler interface {
	Ping(conn net.Conn)
	Echo(conn net.Conn, res []string)
	Set(conn net.Conn, res []string)
	Get(conn net.Conn, res []string)
	INCR(conn net.Conn, res []string)
	RPush(conn net.Conn, res []string)
	LPush(conn net.Conn, res []string)
	LPop(conn net.Conn, res []string)
	BLPop(conn net.Conn, res []string)
	LRange(conn net.Conn, res []string)
	LLen(conn net.Conn, res []string)
	Type(conn net.Conn, res []string)
	XAdd(conn net.Conn, res []string)
	XRange(conn net.Conn, res []string)
	XRead(conn net.Conn, res []string)
	XReadBlock(conn net.Conn, res []string)
}

type Handler struct {
	RedisMap  map[string]*RedisValue
	ListMap   queue.Queue
	StreamMap stream.Stream
	Mu        sync.RWMutex
}

func NewHandler() IHandler {
	return &Handler{
		RedisMap:  make(map[string]*RedisValue),
		ListMap:   queue.NewQueue(),
		StreamMap: stream.NewStream(),
	}
}
