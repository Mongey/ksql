package ksql

import (
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	kafka "github.com/Mongey/terraform-provider-kafka/kafka"
)

// CONFIG

type testConfig struct {
	ksqlServerHost string
	kafkaBootstrapServers []string
}

var config = testConfig{
	ksqlServerHost: "http://localhost:8088",
	kafkaBootstrapServers: []string{"localhost:9092"},
}

// TESTS

func TestNewClient(t *testing.T) {
	_ = createNewClient(t)
}

func TestCreateTable(t *testing.T) {
	c := createNewClient(t)
	defer deleteAllTablesAndStreams(t, c)
}

func TestCreateAndListAndDescribeStream(t *testing.T) {
	c := createNewClient(t)
	defer deleteAllTablesAndStreams(t, c)
	
	usersTopic := createSimpleTopic(t, "users")
	defer usersTopic.Delete()

	userUpdateStream := createTestStream(t, c, &CreateStreamRequest{
		Name: "userUpdates",
		Fields: map[string]string{
			"gender": "string",
		},
		Settings: map[string]string{
			"kafka_topic":  usersTopic.Name,
			"value_format": "JSON",
		},
	})
	defer userUpdateStream.Drop()
	
	testListStreams(t, c, []string{userUpdateStream.Name})
	testDescribe(t, c, userUpdateStream.Name)
}

func TestCreateAsSelectTerminateOnDrop(t *testing.T) {
	c := createNewClient(t)

	usersTopic := createSimpleTopic(t, "someTopic")
	defer usersTopic.Delete()

	someStream := createTestStream(t, c, &CreateStreamRequest{
		Name: "someStream",
		Fields: map[string]string{
			"gender": "string",
		},
		Settings: map[string]string{
			"kafka_topic":  usersTopic.Name,
			"value_format": "JSON",
		},
	})

	persistentStreamingQuery := &streamAsSelect{
		stream: stream{
			client: c,
			CreateStreamRequest: CreateStreamRequest{
				Name: "persistentQueryStream",
			},
		},
		query: fmt.Sprintf("SELECT * FROM %s", someStream.Name),
	}
	err := persistentStreamingQuery.Create()

	if err != nil {
		t.Fatalf("unable to create stream '%s': %v", persistentStreamingQuery.Name, err)
	}

	testListStreams(t, c, []string{someStream.Name, persistentStreamingQuery.Name})

	err = persistentStreamingQuery.Drop()
	if err != nil {
		t.Fatalf("unable to drop stream '%s': %v", persistentStreamingQuery.Name, err)
	}

	testListStreams(t, c, []string{someStream.Name})

	err = someStream.Drop()
	if err != nil {
		t.Fatalf("unable to drop stream '%s': %v", someStream.Name, err)
	}

	testListStreams(t, c, []string{})
}

func TestListEmptyStreams(t *testing.T) {
	c := createNewClient(t)
	defer deleteAllTablesAndStreams(t, c)
	testListStreams(t, c, []string{})
}

// SUB-TESTS (INPUTS REQUIRED)

func testDescribe(t *testing.T, c *Client, name string) {
	desc, err := c.Describe(name)
	if err != nil {
		t.Fatal(err)
	}

	expectResourceName(t, name, desc.Name)

	log.Printf("[TEST] DESCRIBE '%s': %+v", name, desc)
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
				log.Printf("[TEST] stream %d: %s", i, s.Name)
				found = true
				expectedStreams = removeFromSlice(expectedStreams, j)
				break
			}
		}
		if !found {
			t.Errorf("unexpected stream: %s", s.Name)
		}
	}

	// Check for missing streams
	if len(expectedStreams) > 0 {
		t.Errorf("missing expected streams: %v", expectedStreams)
	}

}

// TEST ROUTINES
// All functions belows should:
//  - Receive 't *testing.T' in input
//  - Log (t.Error) or fail (t.Fatal) if an unexpected error occures

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

func createNewClient(t *testing.T) *Client {
	host := config.ksqlServerHost
	c := NewClient(host)
	_, err := c.Info()
	if err != nil {
		t.Fatalf("unable to reach client at '%s': insure the KSQL server is up and reachable", host)
	}
	return c
}

func createNewKafkaClient(t *testing.T) *kafka.Client {
	bootstrapServers := &config.kafkaBootstrapServers
	kc, err := kafka.NewClient(&kafka.Config{
		BootstrapServers: bootstrapServers,
		Timeout:          10,
	})
	if err != nil {
		t.Fatalf("unable to create kafka client at '%v': %v", &config.kafkaBootstrapServers, err)
	}
	return kc
}

type topic struct {
	Name string
	kc   *kafka.Client
}

func (t *topic) Delete() error {
	return t.kc.DeleteTopic(t.Name)
}

func createSimpleTopic(t *testing.T, topicName string) *topic {
	kc := createNewKafkaClient(t)
	err := kc.CreateTopic(kafka.Topic{
		Name:              topicName,
		Partitions:        1,
		ReplicationFactor: 1,
	})
	topic := &topic{
		kc: kc,
		Name: topicName,
	}
	if err != nil {
		t.Fatalf("unable to create topic '%s': %v", topic.Name, err)
	}
	return topic
}

type stream struct {
	CreateStreamRequest
	client *Client
}

func (s *stream) Drop() error {
	return s.client.DropStream(&DropStreamRequest{Name: s.Name})
}

func (s *stream) Exists() bool {
	desc, _ := s.client.Describe(s.Name)
	if desc == nil {
		return false
	}
	return true
}

func createTestStream(t *testing.T, c *Client, req *CreateStreamRequest) *stream {
	err := c.CreateStream(req)
	if err != nil {
		t.Fatalf("unable to create stream '%s': %v", req.Name, err)
	}

	s := &stream{*req, c}
	i := 0

	for {
		if s.Exists() {
			break
		}
		if i > 10 {
			t.Fatalf("unable to create stream '%s': %v", s.Name, err)
		}
		log.Printf("[TEST] waiting on stream '%s' creation", s.Name)
		time.Sleep(1 * time.Second)
		i++
	}

	return s
}

// ASSERTIONS

func expectResourceName(t *testing.T, expected string, real string) {
	expected = strings.ToUpper(expected)
	if expected != real {
		t.Errorf("Expected '%s' as name attribute, but value is '%s'", expected, real)
	}
}

// HELPERS

type streamAsSelect struct {
	stream
	query string
}

func (s *streamAsSelect) Create() error {
	q := fmt.Sprintf("CREATE STREAM %s AS %s;", s.Name, s.query)
	log.Printf("[TEST] %s", q)

	_, err := s.client.Do(Request{KSQL: q})
	return err
}

// removeFromSlice removes a specified index from a slice.
// IMPORTANT: This operation modifies the original order of the array
func removeFromSlice(s []string, i int) []string {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}