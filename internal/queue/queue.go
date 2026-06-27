package queue

import (
	"time"
)

func (q *queue) Len(key string) int {
	return len(q.listMap[key])
}

func (q *queue) RPush(key string, values ...string) int {
	l := q.listMap[key]
	for _, v := range values {
		l = append(l, v)
	}
	q.listMap[key] = l
	if len(q.waiting[key]) > 0 {
		waiter := q.waiting[key][0]
		q.waiting[key] = q.waiting[key][1:len(q.waiting[key])]
		close(waiter.ch)
	}
	return len(l)
}

func (q *queue) LPush(key string, values ...string) int {
	l := q.listMap[key]
	newL := make([]string, 0, len(l)+len(values))
	for i := len(values) - 1; i >= 0; i-- {
		newL = append(newL, values[i])
	}
	newL = append(newL, l...)
	q.listMap[key] = newL
	return len(newL)
}

func (q *queue) LPOP(key string, cnt int) []string {
	s := []string{}
	size := q.Len(key)
	if cnt > size {
		cnt = size
	}
	for cnt > 0 {
		first := q.listMap[key][0]
		q.listMap[key] = q.listMap[key][1:len(q.listMap[key])]
		s = append(s, first)
		cnt--
	}

	return s
}

func (q *queue) CheckExist(key string) ([]string, bool) {
	l, ok := q.listMap[key]
	return l, ok
}

func (q *queue) Set(key string, l []string) {
	q.listMap[key] = l
}

func (q *queue) Query(key string, l, r int) []string {
	arr := q.listMap[key]
	size := len(arr)
	if l < 0 {
		l = size + l
	}
	if l < 0 {
		l = 0
	}
	if r < 0 {
		r = size + r
	}
	if r < 0 {
		r = 0
	}
	if l > size || l > r {
		return []string{}
	} else if r+1 > size {
		r = size - 1
	}
	res := []string{}
	for i := l; i < r+1; i++ {
		res = append(res, arr[i])
	}
	return res
}

func (q *queue) BLPOP(key string, exp float64) []string {
	client := &WaitingClient{
		ch: make(chan struct{}),
	}
	q.waiting[key] = append(q.waiting[key], client)
	if exp == 0 {
		<-client.ch
		res := q.LPOP(key, 1)
		return res
	}
	select {
	case <-client.ch:
		res := q.LPOP(key, 1)
		return res
	case <-time.After(time.Duration(exp) * time.Second):
		return []string{}
	}
}
