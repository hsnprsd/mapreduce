package mapreduce

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"strconv"
	"strings"
)

type Ser interface {
	Serialize(kvs []*KV) []byte
}

type Des interface {
	Deserialize(data []byte) chan *KV
}

type SerDes interface {
	Ser
	Des
}

type JsonSerDes struct {
}

func (sd *JsonSerDes) Serialize(kvs []*KV) []byte {
	data, err := json.Marshal(kvs)
	if err != nil {
		panic(err)
	}
	return data
}

func (sd *JsonSerDes) Deserialize(data []byte) chan *KV {
	result := make(chan *KV)
	go func() {
		defer close(result)

		kvs := make([]*KV, 0)
		err := json.Unmarshal(data, &kvs)
		if err != nil {
			panic(err)
		}
		for _, kv := range kvs {
			result <- kv
		}
	}()
	return result
}

type TextDes struct {
}

func (d *TextDes) Deserialize(data []byte) chan *KV {
	kvs := make(chan *KV)
	go func() {
		defer close(kvs)
		lines := strings.Split(string(data), "\n")
		for i, line := range lines {
			kvs <- &KV{Key: strconv.Itoa(i), Value: line}
		}
	}()
	return kvs
}

type CSVSer struct {
}

func (s *CSVSer) Serialize(kvs []*KV) []byte {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	for _, kv := range kvs {
		w.Write([]string{kv.Key, kv.Value})
	}
	w.Flush()
	return buf.Next(buf.Len())
}
