package main

type tableConfig struct {
	SourceTable      string
	DestinationTable string
	ColumnMapping    map[string]string
}

type partitionValues struct {
	ColumnName string
	Values     []interface{}
	Datatype   string
}

type column struct {
	Name            string
	ClusterSequence int
	Kind            string
	Datatype        string
}
