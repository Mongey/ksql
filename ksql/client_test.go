package ksql

import (
	"log"
	"testing"

	kafka "github.com/Mongey/terraform-provider-kafka/kafka"
)

func TestCreateTable(t *testing.T) {
	c := NewClient("http://localhost:8088")
	defer deleteAllTablesAndStreams(t, c)
}

func TestCreateAndListStreams(t *testing.T) {
	c := NewClient("http://localhost:8088")
	defer deleteAllTablesAndStreams(t, c)

	kc, err := kafka.NewClient(&kafka.Config{
		BootstrapServers: &[]string{"localhost:9092"},
		Timeout:          10,
	})

	if err != nil {
		t.Fatal(err)
	}

	const topic = "users"
	_ = createSimpleTopic(kc, topic)

	defer kc.DeleteTopic(topic)

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

func deleteAllTablesAndStreams(t *testing.T, c *Client) {
	tables, err := c.ListTables()
	if err != nil {
		t.Fatalf("Failed to list streams: %s", err)
	}

	for _, table := range tables {
		c.DropTable(&DropTableRequest{
			Name: table.Name,
		})
	}

	streams, err := c.ListStreams()
	if err != nil {
		t.Fatalf("Failed to list streams: %s", err)
	}

	for _, stream := range streams {
		c.DropStream(&DropStreamRequest{
			Name: stream.Name,
		})
	}
}

func createSimpleTopic(kc *kafka.Client, topicName string) error {
	return kc.CreateTopic(kafka.Topic{
		Name:              topicName,
		Partitions:        1,
		ReplicationFactor: 1,
	})
}
