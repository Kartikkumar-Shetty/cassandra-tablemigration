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

	ips := config.ClusterIPs
	clusterIPs := []string{ips}
	sourceKeySpace := config.SourceKeySpace
	sourceTable := config.SourceTable

	destKeySpace := config.DestinationKeySpace
	destTable := config.DestinationTable

	log.Println("Cluster IP's: ", clusterIPs)

	session, err := createSession(clusterIPs, "system_schema")
	if err != nil {
		log.Println("createSession, Error:", err)
		return
	}
	defer session.Close()

	log.Println("Fetching table Metadata")
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

	log.Println("Fetching Partition Columns keys  for the source Table")
	partitionKeyData, err := getPartitionKeys(sourceKeySpace, sourceTable, sourceTablePartitionColumns[0])
	if err != nil {
		log.Println("getPartitionKeys, Error:", err)
	}
	log.Println("List of keys in the Table: ", sourceTable, ", for Column: ", sourceTablePartitionColumns[0], " ,are :", partitionKeyData)

	log.Println("Fetching source Table Data")

	for _, value := range partitionKeyData {
		data, err := getSourceTableData(session, sourceSelectQuery, sourceTablePartitionColumns[0], value[sourceTablePartitionColumns[0].Name])
		if err != nil {
			log.Println("Error Fetching Data Error:", err)
			return
		}
		log.Println("Data:", data)
	}

}
