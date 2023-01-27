package mapreduce

import (
	"os"
)

type InputReader interface {
	Read(input string) ([]byte, error)
}

type LocalFileInputReader struct {
}

func (i LocalFileInputReader) Read(input string) ([]byte, error) {
	data, err := os.ReadFile(input)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func NewInputReader(input string) (InputReader, error) {
	return LocalFileInputReader{}, nil
}
