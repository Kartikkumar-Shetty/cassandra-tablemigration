package main

import (
	"fmt"
	"testing"
)

func Test_createSourceTableQuery(t *testing.T) {
	type args struct {
		config           tableConfig
		partitionColumns []column
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
		{
			name: "default",
			args: args{
				config: tableConfig{
					SourceTable:      "sourceTableName",
					DestinationTable: "destinantionTableName",
					ColumnMapping: map[string]string{
						"id":    "id1",
						"fname": "fname1",
						"lname": "lname1",
						"p":     "p1",
					},
				},
				partitionColumns: []column{
					{
						Name: "p",
					},
				},
			},
			want: "select fname, lname, p, id  from sourceTableName   where p = ?-",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createSourceTableQuery(tt.args.config, tt.args.partitionColumns); got != tt.want {
				fmt.Println(got)

				//t.Errorf("createSourceTableQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_createDestinationTableQuery(t *testing.T) {
	type args struct {
		config           tableConfig
		partitionColumns []column
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
		{
			name: "default",
			args: args{
				config: tableConfig{
					SourceTable:      "sourceTableName",
					DestinationTable: "destinantionTableName",
					ColumnMapping: map[string]string{
						"id":    "id1",
						"fname": "fname1",
						"lname": "lname1",
						"p":     "p1",
					},
				},
				partitionColumns: []column{
					{
						Name: "p1",
					},
				},
			},
			want: "update table destinantionTableName set  id1 = ?, fname1 = ?, lname1 = ?, p1 = ?  where p1 = ?",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createDestinationTableQuery(tt.args.config, tt.args.partitionColumns); got != tt.want {
				fmt.Println(got)
				//t.Errorf("createDestinationTableQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
