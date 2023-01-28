package main

import (
	"log"
	"strings"
	"time"

	"hsnprsd.fun/mapreduce/mapreduce"
)

func WordCount(kv mapreduce.KV) []mapreduce.KV {
	result := make([]mapreduce.KV, 0)
	words := strings.Split(kv.Value, " ")
	for _, w := range words {
		w = strings.TrimSpace(w)
		result = append(result, mapreduce.KV{Key: w, Value: "1"})
	}
	return result
}

func main() {
	ts := time.Now().UnixMilli()

	ds := mapreduce.NewDataSet(mapreduce.Input{FilePattern: "./input/*", Des: &mapreduce.TextDes{}})
	err := ds.SplitValue().GroupByValue().Count().Write(mapreduce.Output{FileBase: "./output", Ser: &mapreduce.CSVSer{}}, 1)
	if err != nil {
		panic(err)
	}

	endTs := time.Now().UnixMilli()
	log.Printf("TOTAL RUNTIME = %f", float32(endTs-ts)/float32(1000))
}
