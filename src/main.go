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
		log.Println("createSession, Error:", err)
		return
	}
	defer session.Close()

	log.Println("Fetching table")
	sourceTableMetadata, err := getColumnMetadata(sourceKeySpace, sourceTable)

	log.Println("Table metadata", sourceTableMetadata)
	log.Println("Fetching Partition Key")
	partitionColumns := getPartitionColumn(sourceTableMetadata)

	log.Println("Partition Column for table: ", sourceTable, ", is: ", partitionColumns)
	log.Println("SourceKeySpace: ", sourceKeySpace)
	log.Println("SourceTable: ", sourceTable)

	log.Println("Fetching Clustering Key")
	clusteringColumn := getClusterColumn(sourceTableMetadata)
	log.Println("Cluster Columns for table: ", sourceTable, ", is: ", clusteringColumn)

	partitionKeyData, err := getPartitionKeys(sourceKeySpace, sourceTable, partitionColumns[0].Name)
	if err != nil {
		log.Println("getPartitionKeys, Error:", err)
	}
	log.Println("List of keys in the Table: ", sourceTable, ", for Column: ", partitionColumns, " ,are :", partitionKeyData)

}
