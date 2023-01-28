package mapreduce

type DataSet struct {
	input   Input
	mappers []Mapper
	reducer Reducer
}

type GroupedDataSet struct {
	parent *DataSet
}

func (d *DataSet) SplitValue() *DataSet {
	d.mappers = append(d.mappers, SplitValueMapper)
	return d
}

func (d *DataSet) GroupByKey() *GroupedDataSet {
	return &GroupedDataSet{parent: d}
}

func (d *DataSet) GroupByValue() *GroupedDataSet {
	d.mappers = append(d.mappers, SwapKVMapper)
	return &GroupedDataSet{parent: d}
}

func (d *GroupedDataSet) Count() *DataSet {
	d.parent.reducer = CountReducer
	return d.parent
}

func (d *GroupedDataSet) Sum() *DataSet {
	d.parent.reducer = SumReducer
	return d.parent
}

func (d *DataSet) Write(output Output, numPartitions uint32) error {
	mr := MapReduce{
		Input:   d.input,
		Mapper:  ChainMapper(d.mappers...),
		R:       numPartitions,
		Reducer: d.reducer,
		Output:  output,
	}
	return mr.Execute()
}

func NewDataSet(input Input) *DataSet {
	return &DataSet{input: input, mappers: make([]Mapper, 0)}
}
