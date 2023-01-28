package mapreduce

import (
	"fmt"
	"io/ioutil"
	"os"
)

type Mapper func(kv *KV) []*KV

type MapTask struct {
	Input       string
	InputDes    Des
	Mapper      Mapper
	R           uint32
	Partitioner Partitioner
}

type MapTaskResult struct {
	OutputBaseLocation string
	OutputSerDes       SerDes
	Err                error
}

func (m *MapTask) Execute() *MapTaskResult {
	// read input
	data, err := os.ReadFile(m.Input)
	if err != nil {
		return &MapTaskResult{Err: err}
	}
	input := m.InputDes.Deserialize(data)

	// map
	partitions := make([][]*KV, m.R)
	for i := range partitions {
		partitions[i] = make([]*KV, 0)
	}
	for _, i := range input {
		for _, kv := range m.Mapper(i) {
			part := m.Partitioner.Partition(kv.Key)
			partitions[part] = append(partitions[part], kv)
		}
	}

	// write output
	serdes := &JsonSerDes{}

	dir, err := ioutil.TempDir(os.TempDir(), "mapper_output_")
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
