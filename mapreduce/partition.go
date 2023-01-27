package mapreduce

import (
	"hash/fnv"
)

type Partitioner interface {
	Partition(key string) uint32
}

type HashPartitioner struct {
	Mod uint32
}

func (p HashPartitioner) Partition(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % p.Mod
}
