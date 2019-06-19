package ksql

import (
	"log"
	"strings"
	"testing"
	
	kafka "github.com/Mongey/terraform-provider-kafka/kafka"
)

func TestCreateTable(t *testing.T) {
	c := NewClient("http://localhost:8088")
	defer deleteAllTablesAndStreams(t, c)
}

func TestCreateAndListAndDescribeStreams(t *testing.T) {
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
	
	streamName := "userUpdates"
	err = c.CreateStream(&CreateStreamRequest{
		Name: streamName,
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
	
	testListStreams(t, c, []string{streamName})
	testDescribe(t, c, streamName)
}

func TestListStreams(t *testing.T) {
	c := NewClient("http://localhost:8088")
	defer deleteAllTablesAndStreams(t, c)
	testListStreams(t, c, []string{})
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

func expectResourceName(t *testing.T, expected string, real string) {
	expected = strings.ToUpper(expected)
	if expected != real {
		t.Errorf("Expected '%s' as name attribute, but value is '%s'", expected, real)
	}
}

func testDescribe(t *testing.T, c *Client, name string) {
	desc, err := c.Describe(name)
	if err != nil {
		t.Fatal(err)
	}

	expectResourceName(t, name, desc.Name)

	log.Printf("%v", desc)
}

func testListStreams(t *testing.T, c *Client, expectedStreams []string) {
	streams, err := c.ListStreams()
	if err != nil {
		t.Fatal(err)
	}

	// Check for unexpected streams
	for i, s := range streams {
		found := false
		for j, n := range expectedStreams {
			if s.Name == strings.ToUpper(n) {
				log.Printf("Stream %d: %s", i, s.Name)
				found = true
				expectedStreams = removeFromSlice(expectedStreams, j)
				break
			}
		}
		if !found {
			t.Errorf("Unexpected stream: %s", s.Name)
		}
	}

	// Check for missing streams
	if len(expectedStreams) > 0 {
		t.Errorf("Missing expected streams: %v", expectedStreams)
	}

}

// removeFromSlice removes a specified index from a slice.
// IMPORTANT: This operation modifies the original order of the array
func removeFromSlice(s []string, i int) []string {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}