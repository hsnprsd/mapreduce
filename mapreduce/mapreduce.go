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
	R       int
	Reducer Reducer
	Output  Output
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
			Partitioner: HashPartitioner{Mod: m.R},
		}
	}

	mapTasksResults := make([]*MapTaskResult, len(files))
	for i, t := range mapTasks {
		mapTasksResults[i] = t.Execute()
	}

	reduceTasks := make([]ReduceTask, m.R)
	for i := 0; i < m.R; i++ {
		reduceTasks[i] = ReduceTask{
			MapperResults: mapTasksResults,
			Partition:     i,
			Reducer:       m.Reducer,
			Output:        m.Output,
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
