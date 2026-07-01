package stream

type Entry struct {
	ID     string
	Values map[string]string
}

type Stream interface {
	Add(key string, id string, values map[string]string) (string, bool)
	CheckExist(key string) (bool, bool)
	Get(key string) ([]Entry, bool)
}

type stream struct {
	streamMap map[string][]Entry
	lastId    string
}

func NewStream() Stream {
	return &stream{
		streamMap: make(map[string][]Entry),
	}
}
