package main

import (
	"fmt"
	"net"
	"strconv"
)

func handleRPush(conn net.Conn, res []string) {
	length := listMap.RPush(res[1], res[2:]...)
	response := fmt.Sprintf(":%d\r\n", length)
	conn.Write([]byte(response))
}

func handleLPush(conn net.Conn, res []string) {
	length := listMap.LPush(res[1], res[2:]...)
	response := fmt.Sprintf(":%d\r\n", length)
	conn.Write([]byte(response))
}

func handleLPop(conn net.Conn, res []string) {
	arr := []string{}
	if len(res) == 3 {
		cnt, _ := strconv.Atoi(res[2])
		arr = listMap.LPOP(res[1], cnt)
	} else {
		arr = listMap.LPOP(res[1], 1)
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

func handleBLPop(conn net.Conn, res []string) {
	exp, err := strconv.ParseFloat(res[2], 64)
	if err != nil {
		conn.Write([]byte("-ERR invalid timeout\r\n"))
		return
	}
	arr := listMap.BLPOP(res[1], exp)
	if len(arr) == 0 {
		conn.Write([]byte("*-1\r\n"))
	} else {
		response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(res[1]), res[1], len(arr[0]), arr[0])
		conn.Write([]byte(response))
	}
}

func handleLRange(conn net.Conn, res []string) {
	start, _ := strconv.Atoi(res[2])
	end, _ := strconv.Atoi(res[3])
	elements := listMap.Query(res[1], start, end)

	response := fmt.Sprintf("*%d\r\n", len(elements))
	for _, elem := range elements {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem)
	}
	conn.Write([]byte(response))
}

func handleLLen(conn net.Conn, res []string) {
	length := listMap.Len(res[1])
	response := fmt.Sprintf(":%d\r\n", length)
	conn.Write([]byte(response))
}
