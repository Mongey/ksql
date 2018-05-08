package ksql

import (
	"log"
	"testing"
)

func TestListTables(t *testing.T) {
	c := NewClient("http://localhost:8088")
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
