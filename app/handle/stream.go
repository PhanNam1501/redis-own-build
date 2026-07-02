package handle

import (
	"fmt"
	"net"
	"strconv"
)

func (h *Handler) XAdd(conn net.Conn, res []string) {
	keyStream := res[1]
	id := res[2]
	values := make(map[string]string)
	for i := 3; i+1 < len(res); i += 2 {
		values[res[i]] = res[i+1]
	}
	addedId, err := h.StreamMap.Add(keyStream, id, values)
	var response string
	if err != nil {
		response = fmt.Sprintf("-%s\r\n", err.Error())
		conn.Write([]byte(response))
	} else {
		response = fmt.Sprintf("$%d\r\n%s\r\n", len(addedId), addedId)
		conn.Write([]byte(response))
	}
}

func (h *Handler) XRange(conn net.Conn, res []string) {
	key := res[1]
	startId := res[2]
	endId := res[3]
	entries, _ := h.StreamMap.Range(key, startId, endId)

	response := fmt.Sprintf("*%d\r\n", len(entries))
	for _, entry := range entries {
		response += "*2\r\n"
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID), entry.ID)

		kvCount := len(entry.KeyOrder) * 2
		response += fmt.Sprintf("*%d\r\n", kvCount)
		for _, k := range entry.KeyOrder {
			v := entry.Values[k]
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
		}
	}
	conn.Write([]byte(response))
}

func (h *Handler) XRead(conn net.Conn, res []string) {
	numStreams := (len(res) - 2) / 2

	var streamResults []struct {
		key     string
		entries []interface{}
	}

	for i := 0; i < numStreams; i++ {
		keyIdx := 2 + i
		idIdx := 2 + numStreams + i
		key := res[keyIdx]
		id := res[idIdx]

		entries, _ := h.StreamMap.ReadGreater(key, id)

		entriesArray := make([]interface{}, len(entries))
		for j, entry := range entries {
			entryArray := []interface{}{
				entry.ID,
			}
			kvArray := make([]interface{}, 0)
			for _, k := range entry.KeyOrder {
				kvArray = append(kvArray, k, entry.Values[k])
			}
			entryArray = append(entryArray, kvArray)
			entriesArray[j] = entryArray
		}

		streamResults = append(streamResults, struct {
			key     string
			entries []interface{}
		}{key, entriesArray})
	}

	response := fmt.Sprintf("*%d\r\n", len(streamResults))
	for _, stream := range streamResults {
		response += "*2\r\n"
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(stream.key), stream.key)
		response += fmt.Sprintf("*%d\r\n", len(stream.entries))

		for _, entry := range stream.entries {
			entryArray := entry.([]interface{})
			response += "*2\r\n"
			id := entryArray[0].(string)
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(id), id)
			kvArray := entryArray[1].([]interface{})
			response += fmt.Sprintf("*%d\r\n", len(kvArray))
			for _, kv := range kvArray {
				kvStr := kv.(string)
				response += fmt.Sprintf("$%d\r\n%s\r\n", len(kvStr), kvStr)
			}
		}
	}
	conn.Write([]byte(response))
}

func (h *Handler) XReadBlock(conn net.Conn, res []string) {
	timeout, err := strconv.ParseFloat(res[2], 64)
	if err != nil {
		conn.Write([]byte("-ERR invalid timeout\r\n"))
		return
	}

	numStreams := (len(res) - 4) / 2
	var streamResults []struct {
		key     string
		entries []interface{}
	}

	for i := 0; i < numStreams; i++ {
		keyIdx := 4 + i
		idIdx := 4 + numStreams + i
		key := res[keyIdx]
		id := res[idIdx]

		if id == "$" {
			entries, _ := h.StreamMap.Get(key)
			if len(entries) > 0 {
				id = entries[len(entries)-1].ID
			} else {
				id = "0-0"
			}
		}

		entry := h.StreamMap.Block(key, id, timeout)

		if entry.ID == "" {
			conn.Write([]byte("*-1\r\n"))
			continue
		}

		entryArray := []interface{}{entry.ID}
		kvArray := make([]interface{}, 0)
		for _, k := range entry.KeyOrder {
			kvArray = append(kvArray, k, entry.Values[k])
		}
		entryArray = append(entryArray, kvArray)

		streamResults = append(streamResults, struct {
			key     string
			entries []interface{}
		}{key, []interface{}{entryArray}})
	}

	if len(streamResults) > 0 {
		response := fmt.Sprintf("*%d\r\n", len(streamResults))
		for _, stream := range streamResults {
			response += "*2\r\n"
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(stream.key), stream.key)
			response += fmt.Sprintf("*%d\r\n", len(stream.entries))

			for _, entry := range stream.entries {
				entryArray := entry.([]interface{})
				response += "*2\r\n"
				id := entryArray[0].(string)
				response += fmt.Sprintf("$%d\r\n%s\r\n", len(id), id)
				kvArray := entryArray[1].([]interface{})
				response += fmt.Sprintf("*%d\r\n", len(kvArray))
				for _, kv := range kvArray {
					kvStr := kv.(string)
					response += fmt.Sprintf("$%d\r\n%s\r\n", len(kvStr), kvStr)
				}
			}
		}
		conn.Write([]byte(response))
	}
}
