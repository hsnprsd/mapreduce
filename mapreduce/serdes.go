package mapreduce

import "encoding/json"

type Ser interface {
}

type Des interface {
}

type SerDes interface {
	Serialize(kv []KV) []byte
	Deserialize(data []byte) []KV
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
