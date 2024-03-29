package mapreduce

import (
	"fmt"
	"os"
)

type Reducer[T, U any] func(key string, values chan *T) U

type ReduceTask[T, U any] struct {
	MapTasksResults chan *MapTaskResult[T]
	Partition       int
	Reducer         Reducer[T, U]
	Output          Writer[U]
}

type ReduceTaskResult struct {
	Err error
}

func (t *ReduceTask[T, U]) Execute() *ReduceTaskResult {
	// output file
	f, err := os.Create(fmt.Sprintf("%s/part-%d", t.Output.FileBase, t.Partition))
	defer f.Close()
	if err != nil {
		return &ReduceTaskResult{Err: err}
	}

	// read mapper outputs
	input := make(map[string][]T)
	for r := range t.MapTasksResults {
		if r.Err != nil {
			return &ReduceTaskResult{Err: r.Err}
		}
		mapperOutput := fmt.Sprintf("%s/part-%d", r.OutputBaseLocation, t.Partition)
		data, err := os.ReadFile(mapperOutput)
		if err != nil {
			panic(err)
		}
		kvs := r.OutputSerDes.Deserialize(data)
		for kv := range kvs {
			_, ok := input[kv.Key]
			if !ok {
				input[kv.Key] = make([]T, 0)
			}
			input[kv.Key] = append(input[kv.Key], kv.Value)
		}
	}
	// reduce
	for k, vs := range input {
		c := make(chan *T)
		go func() {
			defer close(c)
			for _, v := range vs {
				c <- &v
			}
		}()
		v := t.Reducer(k, c)

		kv := &KV[U]{Key: k, Value: v}
		_, err := f.Write(t.Output.Ser.Serialize(kv))
		if err != nil {
			return &ReduceTaskResult{Err: err}
		}
		_, err = f.Write([]byte("\n"))
		if err != nil {
			return &ReduceTaskResult{Err: err}
		}
	}

	return &ReduceTaskResult{}
}
