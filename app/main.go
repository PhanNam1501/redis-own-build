package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var redisMap map[string]string

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	redisMap = make(map[string]string)
	// Uncomment the code below to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	for {
		r := resp.NewResp(conn)
		res := r.DecodeResp()
		if res == nil {
			break
		}

		if len(res) == 0 {
			continue
		}
		switch {
		case res[0] == "PING":
			conn.Write([]byte("+PONG\r\n"))
		case res[0] == "ECHO" && len(res) > 1:
			response := fmt.Sprintf("$%d\r\n%s\r\n", len(res[1]), res[1])
			conn.Write([]byte(response))
		case res[0] == "SET" && len(res) > 2:
			redisMap[res[1]] = res[2]
			conn.Write([]byte("+OK\r\n"))
		case res[0] == "GET" && len(res) > 1:
			_, ok := redisMap[res[1]]
			if !ok {
				conn.Write([]byte("$-1\r\n"))
			} else {
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(redisMap[res[1]]), redisMap[res[1]])
				conn.Write([]byte(response))
				// conn.Write([]byte(redisMap[res[1]]))
			}
		}
	}
}
