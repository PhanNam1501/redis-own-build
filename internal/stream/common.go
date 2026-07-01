package stream

type Entry struct {
	ID     string
	Values map[string]string
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
