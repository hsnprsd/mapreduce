package mapreduce

import (
	"fmt"
	"os"
)

type Mapper[A, B any] func(kv *KV[A]) []*KV[B]

type MapTask[A, B, C any] struct {
	Input       string
	InputDes    Des[A]
	Mapper      Mapper[A, B]
	Combiner    Reducer[B, C]
	R           uint32
	Partitioner Partitioner
}

type MapTaskResult[T any] struct {
	OutputBaseLocation string
	OutputSerDes       SerDes[T]
	Err                error
}

type keyReduceTask[A any] struct {
	Key    string
	Values chan A
}

func (m *MapTask[A, B, C]) Execute() *MapTaskResult[C] {
	// read input
	data, err := os.ReadFile(m.Input)
	if err != nil {
		return &MapTaskResult[C]{Err: err}
	}
	input := m.InputDes.Deserialize(data)

	// map
	keyReduceTasks := make(chan *keyReduceTask[B])
	go func() {
		defer close(keyReduceTasks)

		keyPartitions := make(map[string][]B)
		for i := range input {
			for _, kv := range m.Mapper(i) {
				if _, ok := keyPartitions[kv.Key]; !ok {
					keyPartitions[kv.Key] = make([]B, 0)
				}
				keyPartitions[kv.Key] = append(keyPartitions[kv.Key], kv.Value)
			}
		}

		for k, vs := range keyPartitions {
			c := make(chan B, len(vs))
			for _, v := range vs {
				c <- v
			}
			close(c)
			keyReduceTasks <- &keyReduceTask[B]{Key: k, Values: c}
		}
	}()

	partitions := make([][]*KV[C], m.R)
	for i := range partitions {
		partitions[i] = make([]*KV[C], 0)
	}
	for t := range keyReduceTasks {
		v := m.Combiner(t.Key, t.Values)
		part := m.Partitioner.Partition(t.Key)
		partitions[part] = append(partitions[part], &KV[C]{Key: t.Key, Value: v})
	}

	// write output
	serdes := &JsonSerDes[C]{}

	dir, err := os.MkdirTemp(os.TempDir(), "mapper_output_")
	if err != nil {
		return &MapTaskResult[C]{Err: err}
	}
	for i, r := range partitions {
		// write result to output location
		f, err := os.Create(dir + "/" + fmt.Sprintf("part-%d", i))
		if err != nil {
			return &MapTaskResult[C]{Err: err}
		}
		f.Write(serdes.Serialize(r))
	}

	return &MapTaskResult[C]{
		OutputBaseLocation: dir,
		OutputSerDes:       serdes,
	}
}
