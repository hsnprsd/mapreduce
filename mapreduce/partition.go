package mapreduce

import (
	"hash/fnv"
)

type Partitioner interface {
	Partition(kv KV) uint32
}

type HashPartitioner struct {
	Mod uint32
}

func (p HashPartitioner) Partition(kv KV) uint32 {
	h := fnv.New32a()
	h.Write([]byte(kv.Key))
	return h.Sum32() % p.Mod
}
