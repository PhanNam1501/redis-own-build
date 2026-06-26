package queue

func (q *queue) RPush(key string, value string) {
	q.listMap[key] = append(q.listMap[key], value)
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
	res := []string{}
	for i := l; i < r; i++ {
		res = append(res, arr[i])
	}
	return res
}
