package ksql

import (
	"log"
	"testing"
)

func TestListTables(t *testing.T) {
	c := NewClient("http://localhost:8088")
	streams, err := c.ListStreams()
	if err != nil {
		t.Fatal(err)
	}

	if len(streams) != 1 {
		t.Errorf("Expected 1 stream, but there's %d", len(streams))
	}
	for i, v := range streams {
		log.Printf("Stream %d: %s", i, v.Name)
	}
}

func TestCreateTableRequest(t *testing.T) {
	req := &CreateTableRequest{
		name: "users_original",
		fields: map[string]string{
			"registertime": "BIGINT",
			"gender":       "VARCHAR",
			"regionid":     "VARCHAR",
			"userid":       "VARCHAR",
		},
		settings: map[string]string{
			"kafka_topic":  "users",
			"value_format": "JSON",
			"key":          "userid",
		},
	}

	r := req.query()
	expected := `CREATE TABLE users_original (registertime BIGINT, gender VARCHAR, regionid VARCHAR, userid VARCHAR) WITH (kafka_topic='users', value_format='JSON', key='userid');`

	if r != expected {
		t.Errorf("\nExpected:%s\nGot:     %s", expected, r)
	}
}
