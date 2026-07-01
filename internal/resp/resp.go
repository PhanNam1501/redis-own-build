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
		if part == "" {
			continue
		}
		// Skip RESP markers: *<number> (array), $<number> (bulk string size)
		// But keep * and $ if they are data values (no digits after)
		if strings.HasPrefix(part, "*") && len(part) > 1 && isDigit(part[1]) {
			continue
		}
		if strings.HasPrefix(part, "$") && len(part) > 1 && isDigit(part[1]) {
			continue
		}
		result = append(result, part)
	}

	return result
}

func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}
