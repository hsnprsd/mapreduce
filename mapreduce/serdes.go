package mapreduce

import (
	"encoding/json"
	"strconv"
	"strings"
)

type Ser[T any] interface {
	Serialize(kvs []*KV[T]) []byte
}

type Des[T any] interface {
	Deserialize(data []byte) chan *KV[T]
}

type SerDes[T any] interface {
	Ser[T]
	Des[T]
}

type JsonSerDes[T any] struct {
}

func (sd *JsonSerDes[T]) Serialize(kvs []*KV[T]) []byte {
	data, err := json.Marshal(kvs)
	if err != nil {
		panic(err)
	}
	return data
}

func (sd *JsonSerDes[T]) Deserialize(data []byte) chan *KV[T] {
	result := make(chan *KV[T])
	go func() {
		defer close(result)

		kvs := make([]*KV[T], 0)
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

func (d *TextDes) Deserialize(data []byte) chan *KV[string] {
	kvs := make(chan *KV[string])
	go func() {
		defer close(kvs)
		lines := strings.Split(string(data), "\n")
		for i, line := range lines {
			kvs <- &KV[string]{Key: strconv.Itoa(i), Value: line}
		}
	}()
	return kvs
}
