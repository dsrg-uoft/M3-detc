package main

import (
	"os"
	"log"
	"time"
	"sync"
	"strings"
	"flag"
	"math/rand"
	"strconv"
	"rialto/detc/client"
	"rialto/detc/common"
)

func main() {
	rand.Seed(int64(os.Getpid()))
	log.Printf("[start] %v", time.Now().Unix())
	var arg_hosts *string = flag.String("hosts", common.Host + ":" + common.Port, "Comma separated hosts.")
	var num_clients *int = flag.Int("clients", 1, "Number of clients.")
	var num_requests *int = flag.Int("requests", 100 * 1000, "Requests per client.")
	var keyspace *int = flag.Int("keys", 100 * 1000, "Number of possible keys.")
	var offset *int = flag.Int("offset", 0, "Key offset.")
	flag.Parse()
	var hosts []string = strings.Split(*arg_hosts, ",")
	var c *client.Client = client.NewClient(hosts)

	var old []byte
	var ret int = c.Put([]byte("key"), []byte("value0"), &old)
	log.Printf("put0 ret %v old %v\n", ret, string(old))

	ret = c.Put([]byte("key"), []byte("value1"), &old)
	log.Printf("put1 ret %v old %v\n", ret, string(old))

	var t uint64 = 0
	ret = c.Get([]byte("key"), &old, &t)
	log.Printf("get ret %v old %v %v t %v diff %v\n", ret, len(old), string(old), t, uint64(time.Now().UnixNano()) - t)

	var wg sync.WaitGroup
	var t0 time.Time = time.Now()
	for i := 0; i < *num_clients; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			var c *client.Client = client.NewClient(hosts)
			for j := 0; j < *keyspace / *num_clients; j++ {
				var key []byte = []byte(strconv.Itoa(rand.Intn(*keyspace) + *offset) + "-key")
				c.Put(key, []byte("some value"), nil)
			}
			wg.Done()
		}(i, &wg)
	}
	wg.Wait()
	var t1 time.Time = time.Now()
	log.Printf("init took %v\n", t1.Sub(t0))
	for i := 0; i < *num_clients; i++ {
		wg.Add(1)
		go func (i int, wg *sync.WaitGroup) {
			var c *client.Client = client.NewClient(hosts)
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
			wg.Done()
		}(i, &wg)
	}
	wg.Wait()
	var t2 time.Time = time.Now()
	log.Printf("took %v, %v\n", t2.Sub(t1), t2.Sub(t0))
	log.Printf("[end] %v", t2.Unix())
}
