package mapreduce

import (
	"strconv"
	"strings"
)

func IdentityMapper(kv KV) []KV {
	return []KV{kv}
}

func SwapKVMapper(kv *KV) []*KV {
	return []*KV{{Key: kv.Value, Value: kv.Key}}
}

func SplitValueMapper(kv *KV) []*KV {
	kvs := make([]*KV, 0)
	for _, v := range strings.Split(kv.Value, " ") {
		kvs = append(kvs, &KV{Key: kv.Key, Value: v})
	}
	return kvs
}

func SumReducer(key string, values chan string) string {
	sum := 0
	for val := range values {
		i, _ := strconv.Atoi(val)
		sum += i
	}
	return strconv.Itoa(sum)
}

func CountReducer(key string, values chan string) string {
	sum := 0
	for {
		x := <-values
		if x == "" {
			break
		}
		sum++
	}
	return strconv.Itoa(sum)
}

func ChainMapper(mappers ...Mapper) Mapper {
	if len(mappers) == 0 {
		return nil
	}
	if len(mappers) == 1 {
		return mappers[0]
	}

	headMapper := mappers[0]
	tailMapper := ChainMapper(mappers[1:]...)

	return func(kv *KV) []*KV {
		allKVs := make([]*KV, 0)
		kvs := headMapper(kv)
		for _, kv := range kvs {
			allKVs = append(allKVs, tailMapper(kv)...)
		}
		return allKVs
	}
}
