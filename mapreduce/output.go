package mapreduce

import (
	"encoding/csv"
	"io"
)

type OutputWriter interface {
	Write(w io.Writer, kvs []KV) error
}

type CSVOutputWriter struct {
}

func (cw *CSVOutputWriter) Write(w io.Writer, kvs []KV) error {
	csvWriter := csv.NewWriter(w)
	for _, kv := range kvs {
		err := csvWriter.Write([]string{kv.Key, kv.Value})
		if err != nil {
			return err
		}
	}
	csvWriter.Flush()
	return nil
}
