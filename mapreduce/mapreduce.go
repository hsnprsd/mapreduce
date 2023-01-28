package mapreduce

import (
	"log"
	"path/filepath"
)

type Input struct {
	FilePattern string
	Des         Des
}

type KV struct {
	Key   string
	Value string
}

type Output struct {
	FileBase string
	Ser      Ser
}

type MapReduce struct {
	Input   Input
	Mapper  Mapper
	R       uint32
	Reducer Reducer
	Output  Output
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
			InputDes:    m.Input.Des,
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

	reduceTasks := make([]ReduceTask, m.R)
	log.Printf("Executing %d total reduce tasks", len(reduceTasks))

	reducersMapTasksResults := make([]chan *MapTaskResult, m.R)
	for i := range reduceTasks {
		reducersMapTasksResults[i] = make(chan *MapTaskResult, len(mapTasks))
		reduceTasks[i] = ReduceTask{
			MapTasksResults: reducersMapTasksResults[i],
			Partition:       i,
			Reducer:         m.Reducer,
			Output:          m.Output,
		}
	}
	go func() {
		for i := 0; i < int(len(mapTasks)); i++ {
			r := <-mapTasksResults
			log.Printf("%d out of %d map tasks finished", i+1, len(mapTasks))
			for i := range reducersMapTasksResults {
				reducersMapTasksResults[i] <- r
			}
		}
		for i := range reducersMapTasksResults {
			close(reducersMapTasksResults[i])
		}
	}()

	reduceTasksResults := make(chan *ReduceTaskResult, len(reduceTasks))
	defer close(reduceTasksResults)
	for _, t := range reduceTasks {
		go func(t ReduceTask) {
			reduceTasksResults <- t.Execute()
		}(t)
	}

	for i := 0; i < len(reduceTasks); i++ {
		r := <-reduceTasksResults
		log.Printf("%d out of %d reduce tasks finished", i+1, len(reduceTasks))
		if r.Err != nil {
			return r.Err
		}
	}

	return nil
}
