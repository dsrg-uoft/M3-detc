package server

import (
	"log"
	"sync"
	"time"
	"bytes"
	"container/list"
	"hash"
	"hash/fnv"
)

const KeySize uint64 = 64
const ValueSize uint64 = 1024
const HeapMax uint64 = 128 * 1024 * 1024 * 1024
const SlabSize uint64 = 1024 * 1024
const LRUSlots uint64 = 16
const MapHeight uint64 = HeapMax / (LRUSlots * ValueSize)
const SlabHeight uint64 = SlabSize / ValueSize
const SlotMax uint64 = HeapMax / SlabSize

type key struct {
	data [KeySize]byte
	size int
}

type value struct {
	data [ValueSize]byte
	size int
}

type KeyEntry struct {
	k *key
	entry *SlabEntry
	home *Slab
	lru_timestamp int64
	row *HashtableRow
}

type SlabEntry struct {
	v value
	timestamp int64
	entry *KeyEntry
}

type Slab struct {
	entries [SlabHeight]SlabEntry
	index int
	counter_hits int
}

type HashtableRow struct {
	entries [LRUSlots]KeyEntry
	lock sync.RWMutex
}

type Hashtable struct {
	rows [MapHeight]HashtableRow
}

type Cache struct {
	table Hashtable
	slabs list.List
	lock sync.RWMutex
	MaxSlabs uint64
	count_slabs uint64
}

/*
 * returns:
 * - 1 if key not found
 * - 0 if key found
 */
func (this *Cache) Get(k []byte, v *[]byte, t *int64) int {
	var h hash.Hash64 = fnv.New64a()
	h.Write(k)
	var row *HashtableRow = &this.table.rows[h.Sum64() % MapHeight]
	row.lock.RLock()
	defer row.lock.RUnlock()
	for i := 0; i < int(LRUSlots); i++ {
		var e *KeyEntry = &row.entries[i]
		if e.k != nil && bytes.Equal(e.k.data[:e.k.size], k) {
			e.home.counter_hits++
			copy(*v, e.entry.v.data[:e.entry.v.size])
			*v = (*v)[:e.entry.v.size]
			if t != nil {
				*t = e.entry.timestamp
			}
			e.lru_timestamp = time.Now().UnixNano()
			return 0
		}
	}
	*v = (*v)[0:0]
	return 1
}

/*
 * returns:
 * - 0 for fun
 */
func (this *Cache) Put(k []byte, v []byte, old *[]byte, s **Slab) int {
	var h hash.Hash64 = fnv.New64a()
	h.Write(k)
	var row *HashtableRow = &this.table.rows[h.Sum64() % MapHeight]
	row.lock.Lock()
	var found bool = false
	var e *KeyEntry = &row.entries[0]
	for i := 0; i < int(LRUSlots); i++ {
		var cur *KeyEntry = &row.entries[i]
		if cur.k != nil && bytes.Equal(cur.k.data[:cur.k.size], k) {
			e = cur
			found = true
			break
		} else if cur.k == nil {
			e = cur
		} else if e.k != nil && e.lru_timestamp > cur.lru_timestamp {
			e = cur
		}
	}

	if *s == nil {
		*s = &Slab{}
	}
	if !found {
		if e.k == nil {
			(*s).entries[(*s).index].entry = e
			e.home = *s
			e.entry = &(*s).entries[(*s).index]
			(*s).index++
			e.row = row
			e.k = &key{}
		}
		key_copy(e.k, k)
	} else {
		if old != nil {
			copy(*old, e.entry.v.data[:e.entry.v.size])
			*old = (*old)[:e.entry.v.size]
		}
	}
	var t int64 = time.Now().UnixNano()
	e.lru_timestamp = t
	e.entry.timestamp = t
	value_copy(&e.entry.v, v)
	row.lock.Unlock()

	if (*s).index == int(SlabHeight) {
		if this.count_slabs >= this.MaxSlabs {
			log.Printf("[debug] %d/%d slabs -> evicting", this.count_slabs, this.MaxSlabs)
			this.evict()
		}
		this.Retire(*s)
		*s = &Slab{}
		if this.count_slabs % 100 == 0 {
			log.Printf("[debug] expanded this.count_slabs %d", this.count_slabs)
		}
	}
	return 0
}

func (this *Cache) Retire(s *Slab) {
	this.lock.Lock()
	this.slabs.PushBack(s)
	this.lock.Unlock()
	this.count_slabs++
}

func (this *Cache) evict() bool {
	this.lock.Lock()
	defer this.lock.Unlock()
	var evictee *list.Element = this.slabs.Front()
	for s := this.slabs.Front(); s != nil; s = s.Next() {
		if s.Value.(*Slab).counter_hits < evictee.Value.(*Slab).counter_hits {
			evictee = s
			if evictee.Value.(*Slab).counter_hits == 0 {
				break
			}
		}
	}
	if evictee != nil {
		this.slabs.Remove(evictee)
		var s *Slab = evictee.Value.(*Slab)
		for i := 0; i < s.index; i++ {
			s.entries[i].entry.row.lock.Lock()
			s.entries[i].entry.k = nil
			s.entries[i].entry.home = nil
			s.entries[i].entry.entry = nil
			s.entries[i].entry.row.lock.Unlock()
		}
		this.count_slabs--
		return true
	}
	return false
}

func (this *Cache) Shrink(percent float32) {
	log.Printf("[debug] evicting, have %d slabs.", this.count_slabs)
	var evicted uint64 = 0
	for i := 0; i < int(float32(this.count_slabs) * percent / 100); i++ {
		if this.evict() {
			evicted++
		}
	}
	log.Printf("[debug] evicted %d slabs, count_slabs %d, percent %f.", evicted, this.count_slabs, percent)
}

func key_copy(dst *key, src []byte) {
	if len(src) > int(KeySize) {
		log.Fatalf("[error] key slice > %d bytes", KeySize)
	}
	copy(dst.data[:], src)
	dst.size = len(src)
}

func value_copy(dst *value, src []byte) {
	if len(src) > int(ValueSize) {
		log.Fatalf("[error] value slice > %d bytes", ValueSize)
	}
	copy(dst.data[:], src)
	dst.size = len(src)
}

func (this *Cache) SlabCounterDec() {
	for {
		this.lock.RLock()
		for e := this.slabs.Front(); e != nil; e = e.Next() {
			var s *Slab = e.Value.(*Slab)
			if s.counter_hits > 0 {
				s.counter_hits -= 10
			}
		}
		this.lock.RUnlock()
		time.Sleep(1 * time.Second)
	}
}
