package queue

import "container/list"

func (q *queue) RPush(key string, value string) {
	q.listMap[key].PushBack(value)
}

func (q *queue) CheckExist(key string) (*list.List, bool) {
	l, ok := q.listMap[key]
	return l, ok
}

func (q *queue) Set(key string, l *list.List) {
	q.listMap[key] = l
}
