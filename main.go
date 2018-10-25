package main

import (
	"log"

	"github.com/Mongey/ksql/ksql"
)

func main() {
	c := ksql.NewClient("http://localhost:8088")

	streams, err := c.ListStreams()

	if err != nil {
		log.Fatal(err)
	}

	for _, stream := range streams {
		c.DropStream(&ksql.DropStreamRequest{
			Name: stream.Name,
		})
	}

	tables, err := c.ListTables()
	if err != nil {
		log.Fatal(err)
	}

	for _, table := range tables {
		c.DropTable(&ksql.DropTableRequest{
			Name: table.Name,
		})
	}
}
