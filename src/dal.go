package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/gocql/gocql"
)

var session *gocql.Session

func executeQuery1(query string) ([]map[string]interface{}, error) {
	return nil, nil
}

func createSession(clusterIPs []string, keyspace string) (*gocql.Session, error) {
	if session != nil {
		return session, nil
	}
	cluster := gocql.NewCluster(clusterIPs...)
	cluster.Keyspace = keyspace
	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	session = sess
	return session, nil
}

func getPartitionKeys(nameSpaceName string, tableName string, col column) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`SELECT distinct "%s" FROM %s.%s where TOKEN("%s") >= -9223372036854775808 AND TOKEN("%s")  <= 9223372036854775807`, col.Name, nameSpaceName, tableName, col.Name, col.Name)
	log.Println("getPartitionKeys: Fetching partition key")
	data, err := executeQuery(session, query)
	if err != nil {
		log.Println("getPartitionKeys: Error: ", err)
		return nil, err
	}
	log.Println("getPartitionKeys: Partition Data: ", data)
	return data, nil
}

func getColumnMetadata(nameSpaceName string, tableName string) ([]column, error) {
	query := fmt.Sprintf(`SELECT * FROM system_schema.columns where keyspace_name='%s' and table_name = '%s'`, nameSpaceName, tableName)
	data, err := executeQuery(session, query)
	if err != nil {
		return nil, err
	}

	columns := translateData(data)
	return columns, nil

}

func translateData(data []map[string]interface{}) []column {
	var columns []column
	for i := 0; i < len(data); i++ {
		col := column{
			Name:            ToString(data[i]["column_name"]),
			ClusterSequence: ToInt(data[i]["position"]),
			Datatype:        ToString(data[i]["type"]),
			Kind:            ToString(data[i]["kind"]),
		}

		columns = append(columns, col)
	}
	return columns
}

func createSourceTableQuery(config tableConfig, partitionColumns []column) string {
	tablePart := fmt.Sprintf(" from %s.%s ", config.SourceKeySpace, config.SourceTable)
	selectPart := ""
	wherePart := ""
	for colName, _ := range config.ColumnMapping {
		for _, pcol := range partitionColumns {
			if pcol.Name == colName {
				continue
			}
		}
		if selectPart != "" {
			selectPart = fmt.Sprintf("%s, %s", selectPart, colName)
			continue
		}
		selectPart = fmt.Sprintf("select %s", colName)
	}
	for _, pcol := range partitionColumns {
		if wherePart != "" {
			wherePart = fmt.Sprintf(" and %s = $%s ,", pcol.Name, pcol.Name)
			continue
		}
		wherePart = fmt.Sprintf(" where %s = $%s", pcol.Name, pcol.Name)

	}
	query := fmt.Sprintf("%s %s %s", selectPart, tablePart, wherePart)
	return query
}

func createDestinationTableQuery(config tableConfig, partitionColumns []column) string {
	tablePart := fmt.Sprintf("update table %s.%s set", config.DestinationKeySpace, config.DestinationTable)
	updatePart := ""
	wherePart := ""
	for _, colName := range config.ColumnMapping {
		isPartitionKey := false
		for _, pcol := range partitionColumns {
			if pcol.Name == colName {
				isPartitionKey = true
				break
			}
		}
		if isPartitionKey {
			continue
		}

		if updatePart != "" {
			updatePart = fmt.Sprintf("%s, %s = $%s", updatePart, colName, colName)
			continue
		}
		updatePart = fmt.Sprintf(" %s = ?", colName)
	}
	for _, pcol := range partitionColumns {
		if wherePart != "" {
			wherePart = fmt.Sprintf(" and %s = $%s ,", pcol.Name, pcol.Name)
			continue
		}
		wherePart = fmt.Sprintf(" where %s = $%s", pcol.Name, pcol.Name)

	}
	query := fmt.Sprintf("%s %s %s", tablePart, updatePart, wherePart)
	return query
}

func getSourceTableData(session *gocql.Session, selectQuery string, partCol column, partitionValue interface{}) ([]map[string]interface{}, error) {
	finalQuery := ""
	switch t := partitionValue.(type) {
	case int, float64, bool:
		finalQuery = strings.Replace(selectQuery, fmt.Sprint("$", partCol.Name), fmt.Sprint(partitionValue.(int)), -1)
	case string:
		finalQuery = strings.Replace(selectQuery, fmt.Sprint("$", partCol.Name), fmt.Sprint("'", partitionValue.(string), "'"), -1)
	default:
		return nil, fmt.Errorf("Unkown DataType for Partition Column, Data: %v", t)
	}

	return executeQuery(session, finalQuery)

}

func insertDestData(session *gocql.Session, insertQuery string, data []map[string]interface{}, paramCount int) {

}

func executeQuery(session *gocql.Session, query string) ([]map[string]interface{}, error) {
	q := session.Query(query)
	defer q.Release()
	return q.Iter().SliceMap()

}

// func insertQuery(session *gocql.Session, query string) ([]map[string]interface{}, error) {

// }

func getPartitionColumn(columns []column) []column {
	var pcolumns []column
	for _, value := range columns {
		if value.Kind == "partition_key" {
			pcolumns = append(pcolumns, value)
		}
	}
	return pcolumns
}

func getClusterColumn(columns []column) []column {
	var clcolumns []column
	for _, value := range columns {
		if value.Kind == "clustering" {
			clcolumns = append(clcolumns, value)
		}
	}
	return clcolumns
}

func readConf(tableConfigPath string) (tableConfig, error) {
	cfg := tableConfig{}
	fileData, err := ioutil.ReadFile(tableConfigPath)
	if err != nil {
		return cfg, err
	}
	err = json.Unmarshal(fileData, &cfg)
	return cfg, err
}
