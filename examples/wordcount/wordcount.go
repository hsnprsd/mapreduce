package main

import (
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"hsnprsd.fun/mapreduce/mapreduce"
)

func Map(kv *mapreduce.KV[string]) []*mapreduce.KV[int] {
	words := strings.Split(kv.Value, " ")
	result := make([]*mapreduce.KV[int], len(words))
	for i := 0; i < len(words); i++ {
		result[i] = &mapreduce.KV[int]{Key: words[i], Value: 1}
	}
	return result
}

func Combiner(key string, value int, accum int) int {
	return value + accum
}

func Reduce(key string, values chan *int) int {
	sum := 0
	for v := range values {
		sum += *v
	}
	return sum
}

func main() {
	fCPU, _ := os.Create("cpu.pprof")
	defer fCPU.Close()
	pprof.StartCPUProfile(fCPU)
	defer pprof.StopCPUProfile()
	runtime.MemProfileRate = 1024

	ts := time.Now().UnixMilli()

	mr := mapreduce.MapReduce[string, int, int, int]{
		Reader:   mapreduce.Reader[string]{FilePattern: "./input/*.txt", Des: &mapreduce.TextDes{}},
		Mapper:   Map,
		Combiner: Combiner,
		R:        8,
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

	fMem, _ := os.Create("allocs.pprof")
	defer fMem.Close()
	pprof.Lookup("allocs").WriteTo(fMem, 0)
}
