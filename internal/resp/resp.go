package resp

import (
	"net"
	"strings"
)

type Resp struct {
	conn net.Conn
}

func NewResp(conn net.Conn) *Resp {
	return &Resp{conn: conn}
}

func (r *Resp) DecodeResp() []string {
	buf := make([]byte, 1024)
	n, err := r.conn.Read(buf)
	if err != nil {
		return nil
	}

	resp := string(buf[:n])

	parts := strings.Split(resp, "\r\n")
	var result []string

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || strings.HasPrefix(part, "*") || strings.HasPrefix(part, "$") {
			continue
		}
		result = append(result, part)
	}

	return result
}
