package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	f, err := os.OpenFile("cassandra-tablemigration.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)
	log.Println("Starting Migration")

	clusterIPs := []string{"127.0.0.1"}
	sourceKeySpace := ""
	sourceTable := ""

	log.Println("Cluster IP's: ", clusterIPs)

	session, err := createSession(clusterIPs, "system_schema")
	if err != nil {
		log.Println("Error creating session:", err)
		return
	}
	defer session.Close()

	sourceTableMetadata, err := getColumnMetadata(sourceKeySpace, sourceTable)
	partitionColumn, err := getPartitionColumn(sourceTableMetadata)
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Println("Partition Column for table: ", sourceTable, ", is: ", partitionColumn)

	log.Println("SourceKeySpace: ", sourceKeySpace)
	log.Println("SourceTable: ", sourceTable)

	clusteringColumn := getClusterColumn(sourceTableMetadata)
	log.Println("Cluster Columns for table: ", sourceTable, ", is: ", clusteringColumn)

	partitionKeyData, err := getPartitionKeys(sourceKeySpace, sourceTable, partitionColumn)
	if err != nil {
		fmt.Println(err)
	}
	log.Println("List of keys in the Table: ", sourceTable, ", for Column: ", partitionColumn, " ,are :", partitionKeyData)
}
