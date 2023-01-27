package mapreduce

import (
	"fmt"
	"os"
)

type Reducer func(key string, values chan string) string

type ReduceTask struct {
	MapTasksResults chan *MapTaskResult
	Partition       int
	Reducer         Reducer
	Output          Output
}

type ReduceTaskResult struct {
	Err error
}

func (t *ReduceTask) Execute() ReduceTaskResult {
	// read mapper outputs
	input := make(map[string][]string)
	for r := range t.MapTasksResults {
		if r.Err != nil {
			return ReduceTaskResult{Err: r.Err}
		}
		mapperOutput := fmt.Sprintf("%s/part-%d", r.OutputBaseLocation, t.Partition)
		data, err := os.ReadFile(mapperOutput)
		if err != nil {
			panic(err)
		}
		kvs := r.OutputSerDes.Deserialize(data)
		for _, kv := range kvs {
			_, ok := input[kv.Key]
			if !ok {
				input[kv.Key] = make([]string, 0)
			}
			input[kv.Key] = append(input[kv.Key], kv.Value)
		}
	}
	// reduce
	kvs := make([]KV, 0)
	for k, vs := range input {
		c := make(chan string)
		go func() {
			defer close(c)
			for _, v := range vs {
				c <- v
			}
		}()
		v := t.Reducer(k, c)
		kvs = append(kvs, KV{Key: k, Value: v})
	}
	// write output
	f, err := os.Create(fmt.Sprintf("%s/part-%d", t.Output.FileBase, t.Partition))
	if err != nil {
		return ReduceTaskResult{Err: err}
	}
	_, err = f.Write(t.Output.Ser.Serialize(kvs))
	if err != nil {
		return ReduceTaskResult{Err: err}
	}

	return ReduceTaskResult{}
}
