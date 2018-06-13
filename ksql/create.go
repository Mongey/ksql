package ksql

import (
	"fmt"
	"strings"
)

type KSQLType string

const (
	TableType  KSQLType = "TABLE"
	StreamType KSQLType = "STREAM"
)

type CreateRequest struct {
	t        KSQLType
	name     string
	fields   map[string]string
	settings map[string]string
}

func (r *CreateRequest) query() string {
	joined := []string{}
	for k, v := range r.fields {
		joined = append(joined, fmt.Sprintf("%s %s", k, v))
	}
	fields := strings.Join(joined, ", ")

	joined = []string{}
	for k, v := range r.settings {
		joined = append(joined, fmt.Sprintf("%s='%s'", k, v))
	}
	with := strings.Join(joined, ", ")
	return fmt.Sprintf("CREATE %s %s (%s) WITH (%s);", r.t, r.name, fields, with)
}

type CreateTableRequest struct {
	name     string
	fields   map[string]string
	settings map[string]string
}

type CreateStreamRequest struct {
	name     string
	fields   map[string]string
	settings map[string]string
}

func (r *CreateStreamRequest) query() string {
	return (&CreateRequest{
		t:        StreamType,
		fields:   r.fields,
		name:     r.name,
		settings: r.settings}).query()
}

func (r *CreateTableRequest) query() string {
	return (&CreateRequest{
		t:        TableType,
		fields:   r.fields,
		name:     r.name,
		settings: r.settings}).query()
}
