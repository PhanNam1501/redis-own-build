package stream

import (
	"strconv"
	"strings"
)

func (s *stream) Add(key string, id string, values map[string]string) (string, bool) {
	ok := s.validate(s.lastId, id)
	if ok {
		entry := Entry{
			ID:     id,
			Values: values,
		}
		s.streamMap[key] = append(s.streamMap[key], entry)

		s.lastId = id

		return id, true
	} else {
		return "", false
	}

}

func (s *stream) CheckExist(key string) (bool, bool) {
	entries, ok := s.streamMap[key]
	return ok && len(entries) > 0, ok
}

func (s *stream) Get(key string) ([]Entry, bool) {
	entries, ok := s.streamMap[key]
	return entries, ok
}

func (s *stream) validate(lastId, id string) bool {
	v1 := strings.Split(lastId, "-")
	v2 := strings.Split(id, "-")
	tv11, _ := strconv.Atoi(v1[0])
	tv12, _ := strconv.Atoi(v1[1])
	tv21, _ := strconv.Atoi(v2[0])
	tv22, _ := strconv.Atoi(v2[1])
	if tv11 < tv21 {
		return false
	} else if tv11 == tv21 {
		if tv12 <= tv22 {
			return false
		}
	}
	return true
}
