package main

import (
	"log"
	"strings"
	"time"

	"hsnprsd.fun/mapreduce/mapreduce"
)

func Map(kv *mapreduce.KV[string]) []*mapreduce.KV[int] {
	words := strings.Split(kv.Value, " ")
	result := make([]*mapreduce.KV[int], 0)
	for _, w := range words {
		result = append(result, &mapreduce.KV[int]{Key: w, Value: 1})
	}
	return result
}

func Reduce(key string, values chan int) int {
	sum := 0
	for v := range values {
		sum += v
	}
	return sum
}

func main() {
	ts := time.Now().UnixMilli()

	mr := mapreduce.MapReduce[string, int, int, int]{
		Reader:   mapreduce.Reader[string]{FilePattern: "./input/part-0.txt", Des: &mapreduce.TextDes{}},
		Mapper:   Map,
		Combiner: Reduce,
		R:        4,
		Reducer:  Reduce,
		Writer: mapreduce.Writer[int]{
			FileBase: "./output",
			Ser:      &mapreduce.JsonSerDes[int]{},
		},
	}
	err := mr.Execute()
	if err != nil {
		panic(err)
	}

	endTs := time.Now().UnixMilli()
	log.Printf("TOTAL RUNTIME = %f", float32(endTs-ts)/float32(1000))
}
