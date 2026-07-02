package handle

import (
	"fmt"
	"net"
	"strconv"
)

func (h *Handler) RPush(conn net.Conn, res []string) {
	length := h.ListMap.RPush(res[1], res[2:]...)
	response := fmt.Sprintf(":%d\r\n", length)
	conn.Write([]byte(response))
}

func (h *Handler) LPush(conn net.Conn, res []string) {
	length := h.ListMap.LPush(res[1], res[2:]...)
	response := fmt.Sprintf(":%d\r\n", length)
	conn.Write([]byte(response))
}

func (h *Handler) LPop(conn net.Conn, res []string) {
	arr := []string{}
	if len(res) == 3 {
		cnt, _ := strconv.Atoi(res[2])
		arr = h.ListMap.LPOP(res[1], cnt)
	} else {
		arr = h.ListMap.LPOP(res[1], 1)
	}
	if len(res) == 2 {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(arr[0]), arr[0])
		conn.Write([]byte(response))
	} else {
		response := fmt.Sprintf("*%d\r\n", len(arr))
		for _, elem := range arr {
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem)
		}
		conn.Write([]byte(response))
	}
}

func (h *Handler) BLPop(conn net.Conn, res []string) {
	exp, err := strconv.ParseFloat(res[2], 64)
	if err != nil {
		conn.Write([]byte("-ERR invalid timeout\r\n"))
		return
	}
	arr := h.ListMap.BLPOP(res[1], exp)
	if len(arr) == 0 {
		conn.Write([]byte("*-1\r\n"))
	} else {
		response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(res[1]), res[1], len(arr[0]), arr[0])
		conn.Write([]byte(response))
	}
}

func (h *Handler) LRange(conn net.Conn, res []string) {
	start, _ := strconv.Atoi(res[2])
	end, _ := strconv.Atoi(res[3])
	elements := h.ListMap.Query(res[1], start, end)

	response := fmt.Sprintf("*%d\r\n", len(elements))
	for _, elem := range elements {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem)
	}
	conn.Write([]byte(response))
}

func (h *Handler) LLen(conn net.Conn, res []string) {
	length := h.ListMap.Len(res[1])
	response := fmt.Sprintf(":%d\r\n", length)
	conn.Write([]byte(response))
}
