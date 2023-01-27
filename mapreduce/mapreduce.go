package mapreduce

import (
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
	// TODO
	files, err := filepath.Glob(m.Input.FilePattern)
	if err != nil {
		return err
	}
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
	for _, t := range mapTasks {
		go func(t MapTask) {
			mapTasksResults <- t.Execute()
		}(t)
	}

	mapTasksResultsExpanded := expand(mapTasksResults, uint32(len(mapTasks)), m.R)

	reduceTasks := make([]ReduceTask, m.R)
	for i := range reduceTasks {

		reduceTasks[i] = ReduceTask{
			MapTasksResults: mapTasksResultsExpanded[i],
			Partition:       i,
			Reducer:         m.Reducer,
			Output:          m.Output,
		}
	}

	for _, t := range reduceTasks {
		result := t.Execute()
		if result.Err != nil {
			return result.Err
		}
	}

	return nil
}
