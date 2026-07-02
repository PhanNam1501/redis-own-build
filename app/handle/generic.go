package handle

import (
	"net"
)

func (h *Handler) Type(conn net.Conn, res []string) {
	h.Mu.RLock()
	_, stringExists := h.RedisMap[res[1]]
	_, listExists := h.ListMap.CheckExist(res[1])
	streamExists, _ := h.StreamMap.CheckExist(res[1])
	h.Mu.RUnlock()

	var response string
	if stringExists {
		response = "+string\r\n"
	} else if listExists {
		response = "+list\r\n"
	} else if streamExists {
		response = "+stream\r\n"
	} else {
		response = "+none\r\n"
	}
	conn.Write([]byte(response))
}
