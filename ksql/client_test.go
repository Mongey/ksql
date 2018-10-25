package ksql

import (
	"log"
	"testing"

	kafka "github.com/Mongey/terraform-provider-kafka/kafka"
)

func createSimpleTopic(kc *kafka.Client, topicName string) error {
	return kc.CreateTopic(kafka.Topic{
		Name:              topicName,
		Partitions:        1,
		ReplicationFactor: 1,
	})
}

func TestCreateAndListStreams(t *testing.T) {
	kc, err := kafka.NewClient(&kafka.Config{
		BootstrapServers: &[]string{"localhost:9092"},
		Timeout:          10,
	})
	if err != nil {
		t.Fatal(err)
	}

	const topic = "users"
	err = createSimpleTopic(kc, topic)
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient("http://localhost:8088")
	defer deleteAllTablesAndStreams(t, c)

	err = c.CreateStream(&CreateStreamRequest{
		Name: "userUpdates",
		Fields: map[string]string{
			"gender": "string",
		},
		Settings: map[string]string{
			"kafka_topic":  topic,
			"value_format": "JSON",
		},
	})

	if err != nil {
		t.Fatal(err)
	}

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

func deleteAllTablesAndStreams(t *testing.T, c *Client) {
	streams, err := c.ListStreams()
	if err != nil {
		t.Fatalf("Failed to list streams: %s", err)
	}

	for _, stream := range streams {
		c.DropStream(&DropStreamRequest{
			Name: stream.Name,
		})
	}
	tables, err := c.ListTables()
	if err != nil {
		t.Fatalf("Failed to list streams: %s", err)
	}

	for _, table := range tables {
		c.DropTable(&DropTableRequest{
			Name: table.Name,
		})
	}
}
