package main

import (
	"encoding/json"
	"errors"
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

func getPartitionKeys(nameSpaceName string, tableName string, colName string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`SELECT distinct "%s" FROM %s.%s where TOKEN("%s") >= -9223372036854775808 AND TOKEN("%s")  <= 9223372036854775807`, colName, nameSpaceName, tableName, colName, colName)
	return executeQuery(session, query)
}

func getColumnMetadata(nameSpaceName string, tableName string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`SELECT * FROM system_schema.columns where keyspace_name='%s' and table_name = '%s'`, nameSpaceName, tableName)
	return executeQuery(session, query)

}

func executeQuery(session *gocql.Session, query string) ([]map[string]interface{}, error) {
	q := session.Query(query)
	defer q.Release()
	return q.Iter().SliceMap()

}

func getPartitionColumn(data []map[string]interface{}) (string, error) {
	for _, value := range data {
		if value["kind"] == "partition_key" {
			key := value["column_name"].(string)
			return key, nil
		}
	}
	return "", errors.New("No Partition key exists")
}

func getClusterColumn(data []map[string]interface{}) map[int]string {
	clusteringColumns := make(map[int]string)
	for _, value := range data {
		if value["kind"] == "clustering" && value["position"] != "-1" {
			pos := value["position"].(int)
			clusteringColumns[pos] = value["column_name"].(string)
		}
	}
	return clusteringColumns
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
