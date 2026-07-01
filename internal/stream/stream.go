package stream

import (
	"strconv"
	"strings"
)

func (s *stream) Add(key string, id string, values map[string]string) (string, error) {
	if id == "0-0" {
		return "", &IDError{Message: "ERR The ID specified in XADD must be greater than 0-0"}
	}

	lastId := s.lastIdMap[key]
	if !s.validate(lastId, id) {
		return "", &IDError{Message: "ERR The ID specified in XADD is equal or smaller than the target stream top item"}
	}

	entry := Entry{
		ID:     id,
		Values: values,
	}
	s.streamMap[key] = append(s.streamMap[key], entry)
	s.lastIdMap[key] = id

	return id, nil
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
	if lastId == "" {
		return true
	}

	v1 := strings.Split(lastId, "-")
	v2 := strings.Split(id, "-")

	if len(v1) != 2 || len(v2) != 2 {
		return false
	}

	tv11, _ := strconv.Atoi(v1[0])
	tv12, _ := strconv.Atoi(v1[1])
	tv21, _ := strconv.Atoi(v2[0])
	tv22, _ := strconv.Atoi(v2[1])

	if tv21 > tv11 {
		return true
	} else if tv21 == tv11 {
		return tv22 > tv12
	}
	return false
}
