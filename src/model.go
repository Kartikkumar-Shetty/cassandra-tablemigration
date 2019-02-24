package main

type tableConfig struct {
	SourceTable                  string
	DestinationTable             string
	DestinationTablePartitionkey string
	ColumnMapping                map[string]string
}

type column struct {
	Name            string
	ClusterSequence int
	Kind            string
	Datatype        string
}
