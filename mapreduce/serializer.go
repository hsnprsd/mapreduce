package mapreduce

import "strings"

func serializeKeyValue(kv KV) []byte {
	return []byte(kv.Key + ":" + kv.Value + ";")
}

func serialize(kvs []KV) []byte {
	result := make([]byte, 0)
	for _, kv := range kvs {
		result = append(result, serializeKeyValue(kv)...)
	}
	return result
}

func deserializeKeyValue(kv string) KV {
	return KV{
		Key:   strings.Split(kv, ":")[0],
		Value: strings.Split(kv, ":")[1],
	}
}

func deserialize(data []byte) []KV {
	result := make([]KV, 0)
	s := string(data)
	for _, x := range strings.Split(s, ";") {
		if x == "" {
			continue
		}
		result = append(result, deserializeKeyValue(x))
	}
	return result
}
