package queue

type Queue interface {
	Len(key string) int
	RPush(key string, values ...string) int
	LPush(key string, values ...string) int
	LPOP(key string, cnt int) []string
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
