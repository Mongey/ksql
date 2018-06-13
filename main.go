package main

import (
	"log"

	"github.com/Mongey/ksql/ksql"
)

func main() {
	c := ksql.NewClient("http://localhost:8088")
	log.Println("=>>> Streams")
	streams, err := c.ListStreams()
	if err != nil {
		log.Fatal(err)
	}
	for i, v := range streams {
		log.Printf("Stream %d: %s", i, v.Name)
	}

	log.Println("=>>> Tables")
	tables, err := c.ListTables()
	if err != nil {
		log.Fatal(err)
	}
	for i, v := range tables {
		log.Printf("Table %d: %s", i, v.Name)
	}

	log.Println("=>>> Limited Query:")
	sql := "SELECT pageid FROM pageviews_original LIMIT 3;"
	q, err := c.LimitQuery(ksql.Request{KSQL: sql})
	if err != nil {
		log.Fatal(err)
	}
	for i, v := range q {
		if v.Row != nil {
			log.Printf("%d:%v\n", i, v.Row)
		}
	}
	log.Println("=>>> Forever Query :")
	ch := make(chan *ksql.QueryResponse)

	go c.Query(ksql.Request{KSQL: "SELECT pageid FROM pageviews_original;"}, ch)

	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case msg := <-ch:
			log.Println(msg.Row)
		}
	}
}
