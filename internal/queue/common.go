package queue

type Queue interface {
	RPush(key string, value string)
	CheckExist(key string) ([]string, bool)
	Set(key string, l []string)
	Query(key string, l, r int) []string
}

type queue struct {
	listMap map[string][]string
}

func NewQueue() Queue {
	return &queue{
		listMap: make(map[string][]string),
	}
}
