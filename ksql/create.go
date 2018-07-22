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
	Name     string
	Fields   map[string]string
	Settings map[string]string
}

type CreateStreamRequest struct {
	Name     string
	Fields   map[string]string
	Settings map[string]string
}

func (r *CreateStreamRequest) query() string {
	return (&CreateRequest{
		t:        StreamType,
		fields:   r.Fields,
		name:     r.Name,
		settings: r.Settings}).query()
}

func (r *CreateTableRequest) query() string {
	return (&CreateRequest{
		t:        TableType,
		fields:   r.Fields,
		name:     r.Name,
		settings: r.Settings}).query()
}
