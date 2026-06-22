package queue

import "container/list"

type Queue interface {
	RPush(key string, value string)
	CheckExist(key string) (*list.List, bool)
	Set(key string, l *list.List)
}

type queue struct {
	listMap map[string]*list.List
}

func NewQueue() Queue {
	return &queue{
		listMap: make(map[string]*list.List),
	}
}
