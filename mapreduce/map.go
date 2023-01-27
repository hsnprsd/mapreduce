package mapreduce

import (
	"fmt"
	"io/ioutil"
	"os"
)

type Mapper func(kv KV) []KV

type MapTask struct {
	Input       string
	Mapper      Mapper
	R           uint32
	Partitioner Partitioner
}

type MapTaskResult struct {
	OutputBaseLocation string
	Err                error
}

func (m *MapTask) Execute() *MapTaskResult {
	result := &MapTaskResult{}

	r, err := NewInputReader(m.Input)
	if err != nil {
		result.Err = err
		return result
	}
	input := r.Read(m.Input)
	if err != nil {
		result.Err = err
		return result
	}
	kvs := make([][]KV, m.R)
	for i := range kvs {
		kvs[i] = make([]KV, 0)
	}
	for i := range input {
		for _, kv := range m.Mapper(i) {
			part := m.Partitioner.Partition(kv)
			kvs[part] = append(kvs[part], kv)
		}
	}

	dir, err := ioutil.TempDir(os.TempDir(), "mapper_output_")
	if err != nil {
		result.Err = err
		return result
	}
	for i, r := range kvs {
		// write result to output location
		f, err := os.Create(dir + "/" + fmt.Sprintf("part-%d", i))
		if err != nil {
			result.Err = err
			return result
		}
		f.Write(serialize(r))
	}

	return &MapTaskResult{
		OutputBaseLocation: dir,
	}
}