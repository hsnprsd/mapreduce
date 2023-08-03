package mapreduce

import (
	"strconv"
	"strings"
)

func IdentityMapper[T any](kv *KV[T]) []*KV[T] {
	return []*KV[T]{kv}
}

func SwapKVMapper(kv *KV[string]) []*KV[string] {
	return []*KV[string]{{Key: kv.Value, Value: kv.Key}}
}

func SplitValueMapper(kv *KV[string]) []*KV[string] {
	kvs := make([]*KV[string], 0)
	for _, v := range strings.Split(kv.Value, " ") {
		kvs = append(kvs, &KV[string]{Key: kv.Key, Value: v})
	}
	return kvs
}

func SumReducer[T int](key string, values chan int) int {
	sum := 0
	for val := range values {
		sum += val
	}
	return sum
}

func CountReducer[T any](key string, values chan T) string {
	sum := 0
	for _, ok := <-values; ok; {
		sum++
	}
	return strconv.Itoa(sum)
}

func ChainMapper[A, B, C any](aMapper Mapper[A, B], bMapper Mapper[B, C]) Mapper[A, C] {
	return func(kv *KV[A]) []*KV[C] {
		result := make([]*KV[C], 0)
		kvs := aMapper(kv)
		for _, kv := range kvs {
			result = append(result, bMapper(kv)...)
		}
		return result
	}
}
