package mapreduce

import (
	"os"
	"strconv"
	"strings"
)

type InputReader interface {
	Read(input string) chan KV
}

type LocalFileInputReader struct {
}

func (i LocalFileInputReader) Read(input string) chan KV {
	result := make(chan KV)

	go func() {
		defer close(result)
		data, err := os.ReadFile(input)
		if err != nil {
			return
		}
		lines := strings.Split(string(data), "\n")

		for i, line := range lines {
			result <- KV{Key: strconv.Itoa(i), Value: line}
		}
	}()

	return result
}

func NewInputReader(input string) (InputReader, error) {
	return LocalFileInputReader{}, nil
}
