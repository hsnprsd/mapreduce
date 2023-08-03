package mapreduce

import (
	"log"
	"path/filepath"
)

type Reader[A any] struct {
	FilePattern string
	Des         Des[A]
}

type KV[T any] struct {
	Key   string
	Value T
}

type Writer[A any] struct {
	FileBase string
	Ser      Ser[A]
}

type MapReduce[I, M, C, R any] struct {
	Reader   Reader[I]
	Mapper   Mapper[I, M]
	Combiner Reducer[M, C]
	R        uint32
	Reducer  Reducer[C, R]
	Writer   Writer[R]
}

func (m *MapReduce[I, M, C, R]) Execute() error {
	// find input files
	files, err := filepath.Glob(m.Reader.FilePattern)
	if err != nil {
		return err
	}
	log.Printf("Found %d total input files", len(files))

	mapTasks := make([]MapTask[I, M, C], len(files))
	for i, file := range files {
		mapTasks[i] = MapTask[I, M, C]{
			Input:       file,
			InputDes:    m.Reader.Des,
			Mapper:      m.Mapper,
			Combiner:    m.Combiner,
			R:           m.R,
			Partitioner: HashPartitioner{Mod: m.R},
		}
	}

	mapTasksResults := make(chan *MapTaskResult[C], len(mapTasks))
	defer close(mapTasksResults)
	log.Printf("Executing %d total map tasks", len(mapTasks))
	for _, t := range mapTasks {
		go func(t MapTask[I, M, C]) {
			mapTasksResults <- t.Execute()
		}(t)
	}

	reduceTasks := make([]ReduceTask[C, R], m.R)
	log.Printf("Executing %d total reduce tasks", len(reduceTasks))

	reducersMapTasksResults := make([]chan *MapTaskResult[C], m.R)
	for i := range reduceTasks {
		reducersMapTasksResults[i] = make(chan *MapTaskResult[C], len(mapTasks))
		reduceTasks[i] = ReduceTask[C, R]{
			MapTasksResults: reducersMapTasksResults[i],
			Partition:       i,
			Reducer:         m.Reducer,
			Output:          m.Writer,
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
		go func(t ReduceTask[C, R]) {
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
