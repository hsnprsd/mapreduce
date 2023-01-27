package mapreduce

import (
	"hash/fnv"
)

type Partitioner interface {
	Partition([]KV) [][]KV
}

type HashPartitioner struct {
	Mod int
}

func (p HashPartitioner) Partition(kvs []KV) [][]KV {
	partitions := make([][]KV, p.Mod)
	for i := range partitions {
		partitions[i] = make([]KV, 0)
	}
	for _, kv := range kvs {
		h := fnv.New32a()
		h.Write([]byte(kv.Key))
		part := h.Sum32() % uint32(p.Mod)
		partitions[part] = append(partitions[part], kv)
	}
	return partitions
}
