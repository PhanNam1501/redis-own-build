package stream

type Entry struct {
	ID       string
	Values   map[string]string
	KeyOrder []string // Track insertion order of keys
}

type IDError struct {
	Message string
}

func (e *IDError) Error() string {
	return e.Message
}

type Stream interface {
	Add(key string, id string, values map[string]string) (string, error)
	CheckExist(key string) (bool, bool)
	Get(key string) ([]Entry, bool)
	Range(key string, startId string, endId string) ([]Entry, bool)
	ReadGreater(key string, id string) ([]Entry, bool)
}

type stream struct {
	streamMap map[string][]Entry
	lastIdMap map[string]string
}

func NewStream() Stream {
	return &stream{
		streamMap: make(map[string][]Entry),
		lastIdMap: make(map[string]string),
	}
}
