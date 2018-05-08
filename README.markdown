# KSQL go client
[![GoDoc](https://godoc.org/github.com/Mongey/ksql/ksql?status.svg)][godoc]
[![CircleCI](https://circleci.com/gh/Mongey/ksql.svg?style=svg)][ci]

A barebones go client for interacting with [Confluent's KSQL][ksql]

## Examples

#### List all Streams

```go
	c := ksql.NewClient("http://localhost:8088")
	log.Println("=>>> Streams")
	streams, err := c.ListStreams()
	if err != nil {
		log.Fatal(err)
	}
	for i, v := range streams {
		log.Printf("Stream %d: %s", i, v.Name)
	}
```

#### List all Tables

```go
	c := ksql.NewClient("http://localhost:8088")
	log.Println("=>>> Tables")
	tables, err := c.ListTables()
	if err != nil {
		log.Fatal(err)
	}
	for i, v := range tables {
		log.Printf("Table %d: %s", i, v.Name)
	}
```

#### Query stream

```go
	c := ksql.NewClient("http://localhost:8088")
	log.Println("=>>> Forever Query :")
	ch := make(chan *ksql.QueryResponse)
	go c.Query(ksql.Request{KSQL: "SELECT pageid FROM pageviews_original;"}, ch)
	for {
		select {
		case msg := <-ch:
			log.Println(msg.Row)
		}
	}
```
[godoc]: https://godoc.org/github.com/Mongey/ksql/ksql
[ksql]: https://www.confluent.io/product/ksql/
[ci]: https://circleci.com/gh/Mongey/ksql
