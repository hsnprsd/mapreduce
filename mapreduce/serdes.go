package mapreduce

import (
	"bytes"
	"encoding/json"
	"strings"
	"unsafe"
)

type Ser[T any] interface {
	Serialize(kv *KV[T]) []byte
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

func (sd *JsonSerDes[T]) Serialize(kv *KV[T]) []byte {
	data, err := json.Marshal(kv)
	if err != nil {
		panic(err)
	}
	return data
}

func (sd *JsonSerDes[T]) Deserialize(data []byte) chan *KV[T] {
	kvs := make(chan *KV[T])
	go func() {
		defer close(kvs)

		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			kv := KV[T]{}
			err := json.Unmarshal([]byte(line), &kv)
			if err != nil {
				panic(err)
			}
			kvs <- &kv
		}
	}()
	return kvs
}

type TextDes struct {
}

func (d *TextDes) Deserialize(data []byte) chan *KV[string] {
	kvs := make(chan *KV[string])
	go func() {
		defer close(kvs)
		lines := bytes.Split(data, []byte("\n"))
		for _, line := range lines {
			kvs <- &KV[string]{Key: "", Value: *(*string)(unsafe.Pointer(&line))}
		}
	}()
	return kvs
}
