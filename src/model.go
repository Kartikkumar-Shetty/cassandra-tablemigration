package main

type tableConfig struct {
	SourceTable                  string
	DestinationTable             string
	DestinationTablePartitionkey string
	ColumnMapping                map[string]string
}

type oolumn struct {
	Name         string
	ClusterOrder int
	ParitionKey  bool
	Datatype     string
}
