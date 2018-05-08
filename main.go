package main

import (
	"log"

	"github.com/Mongey/ksql/ksql"
)

func main() {
	c := ksql.NewClient("http://localhost:8088")

	log.Println("Streams-------------------")
	streams, err := c.ListStreams()

	if err != nil {
		log.Fatal(err)
	}

	for _, stream := range streams {
		log.Printf("Stream Name: %s.\nFormat: %s.\nTopic: %s", stream.Name, stream.Format, stream.Topic)
		//c.DropStream(&ksql.DropStreamRequest{
		//Name: stream.Name,
		//})
	}
	resp, err := c.LimitQuery(ksql.Request{KSQL: "SELECT * FROM VAULT_LOGS_AVRO LIMIT 1;"})

	if err != nil {
		log.Fatal(err)
	}

	for _, r := range resp {
		log.Printf("%v", r.Row)
	}

	log.Println("Tables-------------------")
	tables, err := c.ListTables()
	if err != nil {
		log.Fatal(err)
	}

	for _, table := range tables {
		log.Println(table)
		//c.DropTable(&ksql.DropTableRequest{
		//Name: table.Name,
		//})
	}
}
