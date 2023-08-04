package mapreduce

import (
	"fmt"
	"os"
)

type Mapper[I, T any] func(kv *KV[I]) []*KV[T]
type Combiner[T, U any] func(key string, value T, accum U) U

type MapTask[I, T, C any] struct {
	Input       string
	InputDes    Des[I]
	Mapper      Mapper[I, T]
	Combiner    Combiner[T, C]
	R           uint32
	Partitioner Partitioner
}

type MapTaskResult[T any] struct {
	OutputBaseLocation string
	OutputSerDes       SerDes[T]
	Err                error
}

func (m *MapTask[I, M, C]) Execute() *MapTaskResult[C] {
	// output files
	dir, err := os.MkdirTemp(os.TempDir(), "mapper_output_")
	if err != nil {
		return &MapTaskResult[C]{Err: err}
	}
	outputFiles := make([]*os.File, m.R)
	for i := 0; i < int(m.R); i++ {
		f, err := os.Create(dir + "/" + fmt.Sprintf("part-%d", i))
		defer f.Close()
		if err != nil {
			return &MapTaskResult[C]{Err: err}
		}
		outputFiles[i] = f
	}

	// read input
	data, err := os.ReadFile(m.Input)
	if err != nil {
		return &MapTaskResult[C]{Err: err}
	}
	input := m.InputDes.Deserialize(data)

	kvs := make(map[string]*KV[C])
	for i := range input {
		for _, kv := range m.Mapper(i) {
			if _, ok := kvs[kv.Key]; !ok {
				var accum C
				kvs[kv.Key] = &KV[C]{Key: kv.Key, Value: accum}
			}
			kvs[kv.Key].Value = m.Combiner(kv.Key, kv.Value, kvs[kv.Key].Value)
		}
	}

	serdes := &JsonSerDes[C]{}
	for _, kv := range kvs {
		part := m.Partitioner.Partition(kv.Key)
		_, err := outputFiles[part].Write(serdes.Serialize(kv))
		if err != nil {
			return &MapTaskResult[C]{Err: err}
		}
		_, err = outputFiles[part].Write([]byte("\n"))
		if err != nil {
			return &MapTaskResult[C]{Err: err}
		}
	}

	return &MapTaskResult[C]{
		OutputBaseLocation: dir,
		OutputSerDes:       serdes,
	}
}
