package main

import (
	"log"
	"time"

	"hsnprsd.fun/mapreduce/mapreduce"
)

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
