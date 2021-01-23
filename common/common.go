package common

import (
	"log"
	"net"
)

const Host string = "localhost"
const Port string = "32232"

/*
 * returns
 * - 0 on success
 * - 1 on error
 */
func Read_n_bytes(conn net.Conn, n int, buf []byte) int {
	if cap(buf) < n {
		log.Fatalf("[error] read_n_bytes capacity < n\n")
	}
	for read := 0; read < n; {
		got, err := conn.Read(buf[read:n])
		if err != nil {
			log.Printf("[error] reading %v\n", err.Error())
			return 1
		}
		read += got
	}
	return 0
}
