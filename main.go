package main

import (
	"runtime"
	"log"
	"net"
	"time"
	"strconv"
	"encoding/binary"
	"sync"
	"os"
	"os/signal"
	//"runtime/pprof"
	"syscall"
	"flag"
	"rialto/detc/common"
	"rialto/detc/server"
)

type sigv_info struct {
	enabled bool
	last_low int64
	duration int64
	threshold int
	lock sync.RWMutex
}

type ClientCache struct {
	cache *server.Cache
	slab *server.Slab
}

/*
 * return
 */
func handle_packet(cc *ClientCache, conn net.Conn, info *sigv_info) int {
	var size_buf []byte = make([]byte, 4)
	var err int = common.Read_n_bytes(conn, 4, size_buf)
	if err != 0 {
		return 3
	}
	var size uint32 = binary.BigEndian.Uint32(size_buf)
	var buf []byte = make([]byte, size)
	err = common.Read_n_bytes(conn, int(size), buf)
	if err != 0 {
		return 3
	}
	var cmd int = int(buf[0])
	var ret int
	var res []byte
	switch cmd {
	case 0: // get
		ret = handle_get(cc, buf[1:], &res)
	case 1: // put
		ret = handle_put(cc, buf[1:], &res, info)
	default:
		error_msg(&res, 5)
		return 2
	}
	conn.Write(res)
	return ret
}

func error_msg(res *[]byte, code int) {
	*res = make([]byte, 4 + 1)
	binary.BigEndian.PutUint32(*res, uint32(1))
	(*res)[4] = byte(code)
}

func handle_get(cc *ClientCache, k []byte, res *[]byte) int {
	if len(k) > int(server.KeySize) {
		error_msg(res, 3)
		return 1
	}
	*res = make([]byte, 4 + 1 + 8 + server.ValueSize)
	var data []byte = (*res)[4 + 1 + 8:]
	var t int64 = 0
	var ret int = cc.cache.Get(k, &data, &t)
	binary.BigEndian.PutUint32(*res, uint32(1 + 8 + len(data)))
	(*res)[4] = byte(ret)
	binary.BigEndian.PutUint64((*res)[5:13], uint64(t))
	*res = (*res)[:4 + 1 + 8 + len(data)]
	return 0
}

func handle_put(cc *ClientCache, buf []byte, res *[]byte, info *sigv_info) int {
	var get_old byte = buf[0]
	var k_size int = int(buf[1])
	if k_size > int(server.KeySize) {
		error_msg(res, 3)
		return 1
	}
	buf = buf[2:]
	var k []byte = buf[:k_size]
	buf = buf[k_size:]
	var v []byte = buf
	if len(v) > int(server.ValueSize) {
		error_msg(res, 4)
		return 1
	}
	var res_size int = 0
	if get_old != 0 {
		res_size = int(server.ValueSize)
	}
	*res = make([]byte, 4 + 1 + res_size)
	var data_ptr *[]byte = nil
	var data []byte = (*res)[5:]
	if get_old != 0 {
		data_ptr = &data
	}
	var ret int = cc.cache.Put(k, v, data_ptr, &cc.slab)
	binary.BigEndian.PutUint32(*res, uint32(1 + len(data)))
	(*res)[4] = byte(ret)
	(*res) = (*res)[:4 + 1 + len(data)]
	return 0
}

func handle_request(c *server.Cache, conn net.Conn, info *sigv_info) {
	defer conn.Close()
	var cc ClientCache = ClientCache{cache: c}
	for {
		var err int = handle_packet(&cc, conn, info)
		if err == 3 {
			log.Printf("[info] connection closing\n")
			if cc.slab != nil {
				cc.cache.Retire(cc.slab)
			}
			break
		}
	}
}

func main() {
	var port *string = flag.String("port", common.Port, "not throwing")
	var wounds *int64 = flag.Int64("wounds", 1000 * 1000, "not used")
	log.Printf("[sigve] printing wounds so go can compile %v", wounds)
	var max_size *uint64 = flag.Uint64("size", 8, "size")
	flag.Parse()

	var max_slabs uint64 = *max_size * 1024 * 1024 * 1024 / server.SlabSize
	var c *server.Cache = &server.Cache{MaxSlabs: max_slabs}
	go c.SlabCounterDec()

	var sigs chan os.Signal = make(chan os.Signal, 64)
	var sigve os.Signal = syscall.Signal(64)
	var sigvf os.Signal = syscall.Signal(63)
	var info *sigv_info = &sigv_info{}
	info.last_low = time.Now().UnixNano()
	var go_sigve_percent string = os.Getenv("GO_SIGVE_PERCENT")
	if go_sigve_percent == "" {
		go_sigve_percent = "25"
	}
	threshold, err := strconv.Atoi(go_sigve_percent)
	if err != nil {
		info.threshold = 25
	} else {
		info.threshold = threshold
	}
	var go_sigve string = os.Getenv("GO_SIGVE")
	info.enabled = go_sigve == "1"
	if info.enabled {
		signal.Notify(sigs, sigve, sigvf)
	}
	go func() {
		for true {
			var s os.Signal = <- sigs
			if s == sigve { 
				go func() {
					log.Printf("[sigve] got signal 64")
					var t0 int64 = time.Now().UnixNano()
					info.lock.Lock()
					if float64(info.duration) * 100 / float64(t0 - info.last_low) > float64(info.threshold) {
						info.lock.Unlock()
						return
					}
					info.lock.Unlock()
					c.Shrink(1)
					var t0_0 int64 = time.Now().UnixNano()
					runtime.GC()
					var t0_1 int64 = time.Now().UnixNano()
					runtime.SigVE_Shrink()
					var t1 int64 = time.Now().UnixNano()
					info.lock.Lock()
					info.last_low = t1
					info.duration = t1 - t0
					info.lock.Unlock()
					log.Printf("[sigve] handled %v, took %v, %v, %v, %v", s, info.duration, t0_0 - t0, t0_1 - t0_0, t1 - t0_1)
				}()
			} else {
				go func() {
					log.Printf("[sigve] got signal 63")
					var t0 int64 = time.Now().UnixNano()
					c.Shrink(4)
					var t0_0 int64 = time.Now().UnixNano()
					runtime.GC()
					var t0_1 int64 = time.Now().UnixNano()
					runtime.SigVE_Shrink()
					var t1 int64 = time.Now().UnixNano()
					info.lock.Lock()
					info.last_low = t1
					info.duration = t1 - t0
					info.lock.Unlock()
					log.Printf("[sigve] handled %v, took %v, %v, %v, %v", s, info.duration, t0_0 - t0, t0_1 - t0_0, t1 - t0_1)
				}()
			}
			/*
			f, _ := os.Create("heapdump.bin")
			if err := pprof.WriteHeapProfile(f); err != nil {
				log.Printf("[sigve] could not heap dump: %v", err)
			}
			f.Close()
			*/
		}
	}()

	os.Setenv("GO_SIGVE", "0")
	signal.SigVE_Init()

	var host string = "0.0.0.0"

	l, err := net.Listen("tcp", host + ":" + *port)
	if err != nil {
		log.Fatalf("[error] listening: %v\n", err.Error())
	}
	defer l.Close()

	log.Printf("[info] listening on %v:%v\n", host, *port)
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatalf("[error] accepting: %v\n", err.Error())
		}

		go handle_request(c, conn, info)
	}
}
