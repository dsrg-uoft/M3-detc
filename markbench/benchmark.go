package main

import (
	"runtime"
	"os"
	"os/signal"
	"syscall"
	"log"
	"time"
	"sync"
	"flag"
	"math/rand"
	"strconv"
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

func (this *ClientCache) Get(k []byte, v *[]byte, t *int64) int {
	return this.cache.Get(k, v, t)
}

func (this *ClientCache) Put(k []byte, v []byte, old *[]byte) int {
	return this.cache.Put(k, v, old, &this.slab)
}

func main() {
	rand.Seed(int64(os.Getpid()))
	log.Printf("[start] %v", time.Now().Unix())
	var num_clients *int = flag.Int("clients", 1, "Number of clients.")
	var num_requests *int = flag.Int("requests", 100 * 1000, "Requests per client.")
	var keyspace *int = flag.Int("keys", 100 * 1000, "Number of possible keys.")
	var offset *int = flag.Int("offset", 0, "Key offset.")
	var max_size *uint64 = flag.Uint64("size", 8, "size")
	flag.Parse()

	var max_slabs uint64 = *max_size * 1024 * 1024 * 1024 / server.SlabSize
	var cache *server.Cache = &server.Cache{MaxSlabs: max_slabs}
	go cache.SlabCounterDec()

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
					cache.Shrink(1)
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
					cache.Shrink(4)
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

	var c ClientCache = ClientCache{ cache: cache }

	var old []byte
	var ret int = c.Put([]byte("key"), []byte("value0"), &old)
	log.Printf("put0 ret %v old %v\n", ret, string(old))

	old = make([]byte, server.ValueSize)
	ret = c.Put([]byte("key"), []byte("value1"), &old)
	log.Printf("put1 ret %v old %v\n", ret, string(old))

	var t int64 = 0
	old = make([]byte, server.ValueSize)
	ret = c.Get([]byte("key"), &old, &t)
	log.Printf("get ret %v old %v %v t %v diff %v\n", ret, len(old), string(old), t, time.Now().UnixNano() - t)
	c.cache.Retire(c.slab)

	var wg sync.WaitGroup
	var t0 time.Time = time.Now()
	for i := 0; i < *num_clients; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			var c ClientCache = ClientCache{ cache: cache }
			for j := 0; j < *keyspace / *num_clients; j++ {
				var key []byte = []byte(strconv.Itoa(rand.Intn(*keyspace) + *offset) + "-key")
				c.Put(key, []byte("some value"), nil)
			}
			c.cache.Retire(c.slab)
			wg.Done()
		}(i, &wg)
	}
	wg.Wait()
	var t1 time.Time = time.Now()
	log.Printf("init took %v\n", t1.Sub(t0))
	for i := 0; i < *num_clients; i++ {
		wg.Add(1)
		go func (i int, wg *sync.WaitGroup) {
			var c ClientCache = ClientCache{ cache: cache }
			var hits int = 0
			var misses int = 0
			//var fnv1a hash.Hash64 = fnv.New64a()
			for j := 0; j < *num_requests; j++ {
				//var x []byte = []byte(strconv.Itoa(i * j * 7919))
				//fnv1a.Write([]byte(x))
				//var key []byte = []byte("key-" + strconv.FormatUint(fnv1a.Sum64() % (100 * 1000), 10))
				var key []byte = []byte(strconv.Itoa(rand.Intn(*keyspace) + *offset) + "-key")
				//log.Printf("key is %s\n", string(key))
				ret = c.Get(key, &old, nil)
				//log.Printf("here\n")
				if ret != 0 {
					c.Put(key, []byte("some value"), nil)
					time.Sleep(1 * time.Millisecond)
					misses++
					//log.Printf("there\n")
				} else {
					hits++
				}
				if j % (*num_requests / 20) == 0 {
					log.Printf("- client %v done %v keys", i, j)
				}
			}
			log.Printf("finished with %v hits/%v misses\n", hits, misses)
			c.cache.Retire(c.slab)
			wg.Done()
		}(i, &wg)
	}
	wg.Wait()
	var t2 time.Time = time.Now()
	log.Printf("took %v, %v\n", t2.Sub(t1), t2.Sub(t0))
	log.Printf("[end] %v", t2.Unix())
}
