package mapreduce

import (
	"encoding/json"
	"strconv"
	"strings"
)

type Ser interface {
	Serialize(kv []KV) []byte
}

type Des interface {
	Deserialize(data []byte) []KV
}

type SerDes interface {
	Ser
	Des
}

type JsonSerDes struct {
}

func (sd *JsonSerDes) Serialize(kv []KV) []byte {
	data, err := json.Marshal(kv)
	if err != nil {
		panic(err)
	}
	return data
}

func (sd *JsonSerDes) Deserialize(data []byte) []KV {
	kvs := make([]KV, 0)
	err := json.Unmarshal(data, &kvs)
	if err != nil {
		panic(err)
	}
	return kvs
}

type TextDes struct {
}

func (ds *TextDes) Deserialize(data []byte) []KV {
	kvs := make([]KV, 0)
	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		kvs = append(kvs, KV{Key: strconv.Itoa(i), Value: line})
	}
	return kvs
}
