package main

import (
	"strconv"
	"strings"

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

func Add(key string, values chan string) string {
	sum := 0
	for val := range values {
		i, _ := strconv.Atoi(val)
		sum += i
	}
	return strconv.Itoa(sum)
}

func main() {
	m := mapreduce.MapReduce{
		Input:   mapreduce.Input{FilePattern: "./input/*", Des: &mapreduce.TextDes{}},
		Mapper:  WordCount,
		R:       8,
		Reducer: Add,
		Output:  mapreduce.Output{FileBase: "./output"},
	}
	err := m.Execute()
	if err != nil {
		panic(err)
	}
}
