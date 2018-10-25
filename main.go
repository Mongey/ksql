package main

import (
	"log"

	"github.com/Mongey/ksql/ksql"
	"github.com/Mongey/terraform-provider-kafka/kafka"
)

func main() {
	c := ksql.NewClient("http://localhost:8088")

	cfg := &kafka.Config{
		BootstrapServers: &[]string{"localhost:9092"},
		Timeout:          100,
	}

	kafkaClient, err := kafka.NewClient(cfg)
	if err != nil {
		log.Fatalf("[FATAL] Could not construct Kafka Client")
	}

	err = kafkaClient.CreateTopic(kafka.Topic{
		Name:              "pageviews_original",
		Partitions:        1,
		ReplicationFactor: 1,
		Config:            map[string]*string{},
	})
	defer kafkaClient.DeleteTopic("pageviews_original")

	log.Printf("[DEBUG] Topic 1 creation: %s", err)

	err = kafkaClient.CreateTopic(kafka.Topic{
		Name:              "pageviews",
		Partitions:        1,
		ReplicationFactor: 1,
		Config:            map[string]*string{},
	})
	defer kafkaClient.DeleteTopic("pageviews")

	log.Printf("[DEBUG] Topic 2 creation: %s", err)

	r := ksql.Request{
		KSQL: `CREATE STREAM pageviews_original
	(viewtime bigint, userid varchar, pageid varchar)
	WITH (kafka_topic='pageviews', value_format='DELIMITED');`,
	}

	dr := &ksql.DropStreamRequest{Name: "pageviews_original"}
	defer c.DropStream(dr)

	res, err := c.Do(r)
	log.Printf("[DEBUG] %v | %s", res, err)

	log.Println("=>>> Streams")
	streams, err := c.ListStreams()
	if err != nil {
		log.Fatal(err)
	}

	for i, v := range streams {
		log.Printf("Stream %d: %s", i, v.Name)
	}

	log.Println("=>>> Tables")
	//tables, err := c.ListTables()
	//if err != nil {
	//log.Fatal(err)
	//}
	//for i, v := range tables {
	//log.Printf("Table %d: %s", i, v.Name)
	//}

	//log.Println("=>>> Limited Query:")
	//sql := "SELECT pageid FROM pageviews_original LIMIT 3;"
	//q, err := c.LimitQuery(ksql.Request{KSQL: sql})
	//if err != nil {
	//log.Fatal(err)
	//}

	//for i, v := range q {
	//if v.Row != nil {
	//log.Printf("%d:%v\n", i, v.Row)
	//}
	//}
	info, err := c.Info()
	if err != nil {
		log.Fatalf("[ERROR] %s", err)
	}

	log.Printf("[INFO] Running against %s", info.Version)
}
