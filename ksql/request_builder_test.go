package ksql

import "testing"

func TestCreateTableRequest(t *testing.T) {
	req := &CreateTableRequest{
		Name: "users_original",

		Fields: map[string]string{
			"registertime": "BIGINT",
			"gender":       "VARCHAR",
			"regionid":     "VARCHAR",
			"userid":       "VARCHAR",
		},
		Settings: map[string]string{
			"kafka_topic":  "users",
			"value_format": "JSON",
			"key":          "userid",
		},
	}

	r := req.query()
	expected := `CREATE TABLE users_original (gender VARCHAR, regionid VARCHAR, registertime BIGINT, userid VARCHAR) WITH (kafka_topic='users', key='userid', value_format='JSON');`

	if r != expected {
		t.Errorf("\nExpected:%s\nGot:     %s", expected, r)
	}
}
