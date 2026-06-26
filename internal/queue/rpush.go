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
	size := len(arr)
	if l < 0 {
		l = size + l
	}
	if l < 0 {
		return []string{}
	}
	if r < 0 {
		r = size + r
	}
	if r < 0 {
		return []string{}
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
