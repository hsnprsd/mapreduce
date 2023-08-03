package mapreduce

import (
	"fmt"
	"os"
)

type Mapper func(kv *KV) []*KV

type MapTask struct {
	Input       string
	InputDes    Des
	Mapper      Mapper
	Combiner    Reducer
	R           uint32
	Partitioner Partitioner
}

type MapTaskResult struct {
	OutputBaseLocation string
	OutputSerDes       SerDes
	Err                error
}

type keyReduceTask struct {
	Key    string
	Values chan string
}

func (m *MapTask) Execute() *MapTaskResult {
	// read input
	data, err := os.ReadFile(m.Input)
	if err != nil {
		return &MapTaskResult{Err: err}
	}
	input := m.InputDes.Deserialize(data)

	// map
	keyReduceTasks := make(chan *keyReduceTask)
	go func() {
		defer close(keyReduceTasks)

		keyPartitions := make(map[string][]string)
		for i := range input {
			for _, kv := range m.Mapper(i) {
				if _, ok := keyPartitions[kv.Key]; !ok {
					keyPartitions[kv.Key] = make([]string, 0)
				}
				keyPartitions[kv.Key] = append(keyPartitions[kv.Key], kv.Value)
			}
		}

		for k, vs := range keyPartitions {
			c := make(chan string, len(vs))
			for _, v := range vs {
				c <- v
			}
			close(c)
			keyReduceTasks <- &keyReduceTask{Key: k, Values: c}
		}
	}()

	partitions := make([][]*KV, m.R)
	for i := range partitions {
		partitions[i] = make([]*KV, 0)
	}
	for t := range keyReduceTasks {
		v := m.Combiner(t.Key, t.Values)
		part := m.Partitioner.Partition(t.Key)
		partitions[part] = append(partitions[part], &KV{Key: t.Key, Value: v})
	}

	// write output
	serdes := &JsonSerDes{}

	dir, err := os.MkdirTemp(os.TempDir(), "mapper_output_")
	if err != nil {
		return &MapTaskResult{Err: err}
	}
	for i, r := range partitions {
		// write result to output location
		f, err := os.Create(dir + "/" + fmt.Sprintf("part-%d", i))
		if err != nil {
			return &MapTaskResult{Err: err}
		}
		f.Write(serdes.Serialize(r))
	}

	return &MapTaskResult{
		OutputBaseLocation: dir,
		OutputSerDes:       serdes,
	}
}
