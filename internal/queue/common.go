package queue

import "sync"

type Queue interface {
	Len(key string) int
	RPush(key string, values ...string) int
	LPush(key string, values ...string) int
	LPOP(key string, cnt int) []string
	CheckExist(key string) ([]string, bool)
	Set(key string, l []string)
	Query(key string, l, r int) []string
	BLPOP(key string, exp float64) []string
}
type WaitingClient struct {
	ch chan struct{}
}
type queue struct {
	listMap map[string][]string
	waiting map[string][]*WaitingClient
	mu      sync.RWMutex
}

func NewQueue() Queue {
	return &queue{
		listMap: make(map[string][]string),
		waiting: make(map[string][]*WaitingClient),
		mu:      sync.RWMutex{},
	}
}
