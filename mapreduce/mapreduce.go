package mapreduce

import (
	"log"
	"path/filepath"
)

type Input struct {
	FilePattern string
}

type KV struct {
	Key   string
	Value string
}

type Output struct {
	FileBase string
}

type MapReduce struct {
	Input   Input
	Mapper  Mapper
	R       uint32
	Reducer Reducer
	Output  Output
}

func expand(mapTasksResults chan *MapTaskResult, M, R uint32) []chan *MapTaskResult {
	result := make([]chan *MapTaskResult, R)
	for i := range result {
		result[i] = make(chan *MapTaskResult, M)
	}
	go func() {
		for i := 0; i < int(M); i++ {
			r := <-mapTasksResults
			log.Printf("%d out of %d map tasks finished", i+1, M)
			for i := range result {
				result[i] <- r
			}
		}
		for i := range result {
			close(result[i])
		}
	}()
	return result
}

func (m *MapReduce) Execute() error {
	// find input files
	files, err := filepath.Glob(m.Input.FilePattern)
	if err != nil {
		return err
	}
	log.Printf("Found %d total input files", len(files))

	mapTasks := make([]MapTask, len(files))
	for i, file := range files {
		mapTasks[i] = MapTask{
			Input:       file,
			Mapper:      m.Mapper,
			R:           m.R,
			Partitioner: HashPartitioner{Mod: m.R},
		}
	}

	mapTasksResults := make(chan *MapTaskResult, len(mapTasks))
	defer close(mapTasksResults)
	log.Printf("Executing %d total map tasks", len(mapTasks))
	for _, t := range mapTasks {
		go func(t MapTask) {
			mapTasksResults <- t.Execute()
		}(t)
	}

	mapTasksResultsExpanded := expand(mapTasksResults, uint32(len(mapTasks)), m.R)

	reduceTasks := make([]ReduceTask, m.R)
	log.Printf("Executing %d total reduce tasks", len(reduceTasks))
	for i := range reduceTasks {
		reduceTasks[i] = ReduceTask{
			MapTasksResults: mapTasksResultsExpanded[i],
			Partition:       i,
			Reducer:         m.Reducer,
			Output:          m.Output,
		}
	}

	for i, t := range reduceTasks {
		result := t.Execute()
		log.Printf("%d out of %d reduce tasks finished", i+1, len(reduceTasks))
		if result.Err != nil {
			return result.Err
		}
	}

	return nil
}
