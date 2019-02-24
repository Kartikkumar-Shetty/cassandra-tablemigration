package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

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

func getPartitionKeys(nameSpaceName string, tableName string, colName string) ([]column, error) {
	query := fmt.Sprintf(`SELECT distinct "%s" FROM %s.%s where TOKEN("%s") >= -9223372036854775808 AND TOKEN("%s")  <= 9223372036854775807`, colName, nameSpaceName, tableName, colName, colName)
	data, err := executeQuery(session, query)
	if err != nil {
		return nil, err
	}
	columns := translateData(data)
	return columns, nil
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
			Name:            data[i]["column_name"].(string),
			ClusterSequence: data[i]["position"].(int),
			Datatype:        data[i]["type"].(string),
			Kind:            data[i]["kind"].(string),
		}

		columns = append(columns, col)
	}
	return columns
}

func executeQuery(session *gocql.Session, query string) ([]map[string]interface{}, error) {
	q := session.Query(query)
	defer q.Release()
	return q.Iter().SliceMap()

}

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

func readConf(tableConfigPath string) (*tableConfig, error) {
	cfg := tableConfig{}
	fileData, err := ioutil.ReadFile(tableConfigPath)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(fileData, &cfg)
	return &cfg, err
}
