package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	f, err := os.OpenFile("cassandra-tablemigration.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("error opening file: ", err)
	}
	defer f.Close()

	log.SetOutput(f)
	log.Println("Starting Migration")

	config, err := readConf("./tablemigration.json")
	if err != nil {
		log.Println("Config Read Error, Error:", err)
		return
	}
	path, err := os.Executable()
	if err != nil {
		log.Println("Path Error:", err)
		return
	}
	log.Println("Executable Path:", path)
	log.Println("Config :", config)

	clusterIPs := []string{"127.0.0.1"}
	sourceKeySpace := ""
	sourceTable := "testtable1"

	destKeySpace := ""
	destTable := "testtable2"

	log.Println("Cluster IP's: ", clusterIPs)

	session, err := createSession(clusterIPs, "system_schema")
	if err != nil {
		log.Println("createSession, Error:", err)
		return
	}
	defer session.Close()

	log.Println("Fetching table")
	sourceTableMetadata, err := getColumnMetadata(sourceKeySpace, sourceTable)
	if err != nil {
		log.Println("Error fetching Source Table metadata: Error", err)
		return
	}
	log.Println("Source Table metadata", sourceTableMetadata)

	destTableMetadata, err := getColumnMetadata(destKeySpace, destTable)
	if err != nil {
		log.Println("Error fetching Destination Table metadata: Error", err)
		return
	}
	log.Println("Destinaton Table metadata", destTableMetadata)

	log.Println("Fetching Partition Key")
	sourceTablePartitionColumns := getPartitionColumn(sourceTableMetadata)
	log.Println("Source table Partition Column for table: ", sourceTable, ", is: ", sourceTablePartitionColumns)
	log.Println("SourceKeySpace: ", sourceKeySpace)
	log.Println("SourceTable: ", sourceTable)

	destTablePartitionColumns := getPartitionColumn(destTableMetadata)
	log.Println("Destination table Partition Column for table: ", destTable, ", is: ", destTablePartitionColumns)
	log.Println("DestinationKeySpace: ", destKeySpace)
	log.Println("DestinationTable: ", destTable)

	log.Println("Fetching Clustering Key")
	sourceclusteringColumn := getClusterColumn(sourceTableMetadata)
	log.Println("Cluster Columns for Source table: ", sourceTable, ", is: ", sourceclusteringColumn)

	destclusteringColumn := getClusterColumn(destTableMetadata)
	log.Println("Cluster Columns for Destination table: ", destTable, ", is: ", destclusteringColumn)

	sourceSelectQuery := createSourceTableQuery(config, sourceTablePartitionColumns)
	log.Println(sourceSelectQuery)

	destUpdateQuery := createDestinationTableQuery(config, destTablePartitionColumns)
	log.Println(destUpdateQuery)

	// partitionKeyData, err := getPartitionKeys(sourceKeySpace, sourceTable, partitionColumns[0])
	// if err != nil {
	// 	log.Println("getPartitionKeys, Error:", err)
	// }
	// log.Println("List of keys in the Table: ", sourceTable, ", for Column: ", partitionColumns, " ,are :", partitionKeyData)

}
