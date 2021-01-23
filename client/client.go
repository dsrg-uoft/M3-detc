package client

import (
	"net"
	"log"
	"encoding/binary"
	"hash"
	"hash/fnv"
	"../common"
)

type Client struct {
	hosts []string
	conn_map map[string]*net.Conn
}

func NewClient(hosts []string) *Client {
	var c *Client = &Client{hosts, make(map[string]*net.Conn)}
	return c
}

func (this *Client) get_server(k []byte) *net.Conn {
	var fnv1a hash.Hash64 = fnv.New64a()
	fnv1a.Write(k)
	var host string = this.hosts[fnv1a.Sum64() % uint64(len(this.hosts))]

	var conn *net.Conn = nil
	if this.conn_map[host] == nil {
		dial_conn, err := net.Dial("tcp", host)
		if err != nil {
			log.Printf("[error] dialing: %v %v\n", host, err.Error())
			return nil
		}
		conn = &dial_conn
		this.conn_map[host] = conn
	} else {
		conn = this.conn_map[host]
	}
	return conn
}

func (this *Client) Get(k []byte, v *[]byte, t *uint64) int {
	var conn *net.Conn = this.get_server(k)
	if conn == nil {
		return -1
	}
	var msg []byte = make([]byte, 4 + 1 + len(k))
	binary.BigEndian.PutUint32(msg, uint32(1 + len(k)))
	msg[4] = 0
	copy(msg[5:], k)
	(*conn).Write(msg)

	var size_buf []byte = make([]byte, 4)
	var err int = common.Read_n_bytes(*conn, 4, size_buf)
	if err != 0 {
		return -2
	}
	var size uint32 = binary.BigEndian.Uint32(size_buf)
	var buf []byte = make([]byte, size)
	err = common.Read_n_bytes(*conn, int(size), buf)
	if err != 0 {
		return -2
	}
	if t != nil {
		*t = binary.BigEndian.Uint64(buf[1:9])
	}
	*v = buf[9:]
	return int(buf[0])
}

func (this *Client) Put(k []byte, v []byte, old *[]byte) int {
	var conn *net.Conn = this.get_server(k)
	if conn == nil {
		return -1
	}
	var msg []byte = make([]byte, 4 + 1 + 1 + 1 + len(k) + len(v))
	binary.BigEndian.PutUint32(msg, uint32(1 + 1 + 1 + len(k) + len(v)))
	msg[4] = 1
	if old == nil {
		msg[5] = 0
	} else {
		msg[5] = 1
	}
	msg[6] = byte(len(k))
	copy(msg[7:7 + len(k)], k)
	copy(msg[7 + len(k):], v)
	(*conn).Write(msg)

	var size_buf []byte = make([]byte, 4)
	var err int = common.Read_n_bytes(*conn, 4, size_buf)
	if err != 0 {
		return -2
	}
	var size uint32 = binary.BigEndian.Uint32(size_buf)
	var buf []byte = make([]byte, size)
	err = common.Read_n_bytes(*conn, int(size), buf)
	if err != 0 {
		return -2
	}
	if old != nil && size > 1 {
		*old = buf[1:]
	}
	return int(buf[0])
}
