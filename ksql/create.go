package ksql

import (
	"fmt"
	"sort"
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
	f := r.sortedFields()
	fields := strings.Join(f, ", ")

	s := r.sortedWith()
	with := strings.Join(s, ", ")

	return fmt.Sprintf("CREATE %s %s (%s) WITH (%s);", r.t, r.name, fields, with)
}

func (r *CreateRequest) sortedWith() []string {
	pairs := []string{}

	var sortedFields []string
	for k := range r.settings {
		sortedFields = append(sortedFields, k)
	}
	sort.Strings(sortedFields)

	for _, k := range sortedFields {
		v := r.settings[k]
		pairs = append(pairs, fmt.Sprintf("%s='%s'", k, v))
	}

	return pairs
}

func (r *CreateRequest) sortedFields() []string {
	pairs := []string{}

	var sortedFields []string
	for k := range r.fields {
		sortedFields = append(sortedFields, k)
	}
	sort.Strings(sortedFields)

	for _, k := range sortedFields {
		v := r.fields[k]
		pairs = append(pairs, fmt.Sprintf("%s %s", k, v))
	}

	return pairs
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
