package main

import "net"

func handleType(conn net.Conn, res []string) {
	mu.RLock()
	_, stringExists := redisMap[res[1]]
	_, listExists := listMap.CheckExist(res[1])
	streamExists, _ := streamMap.CheckExist(res[1])
	mu.RUnlock()

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
