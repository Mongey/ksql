package ksql

import (
	"fmt"
	"strings"
)

type ErrorMessage struct {
	Message    string   `json:"message"`
	StackTrace []string `json:"stackTrace"`
}

type QueryResponse struct {
	Row *struct {
		Columns []interface{} `json:"columns"`
	} `json:"row"`
	ErrorMessage ErrorMessage `json:"errorMessage"`
}

type Request struct {
	KSQL                 string            `json:"ksql"`
	StreamsProperties    map[string]string `json:"streamsProperties,omitempty"`
	streamPropertiesName string
}

type Response []IntResponse

type ErrResp struct {
	Type         string   `json:"@type,omitempty"`
	ErrorCode    int      `json:"error_code,omitempty"`
	ErrorMessage string   `json:"message,omitempty"`
	StackTrace   []string `json:"stackTrace,omitempty"`
}

func (e *ErrResp) Error() string {
	stackTrace := strings.Join(e.StackTrace, "\n")
	return fmt.Sprintf("%d [%s]: %s\n StackTrace:\n %s", e.ErrorCode, e.Type, e.ErrorMessage, stackTrace)
}

type IntResponse struct {
	Error *struct {
		StatementText string       `json:"statementText"`
		ErrorMessage  ErrorMessage `json:"errorMessage"`
	} `json:"error,omitempty"`
	Streams *struct {
		StatementText string   `json:"statementText"`
		Streams       []Stream `json:"streams"`
	} `json:"streams,omitempty"`
	Tables *struct {
		StatementText string  `json:"statementText"`
		Tables        []Table `json:"tables"`
	} `json:"tables,omitempty"`

	Status *struct {
		StatementText string `json:"statementText"`
		CommandID     string `json:"commandId"`
		CommandStatus *struct {
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"commandStatus"`
	} `json:"currentStatus"`
}

type StatusResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

type Stream struct {
	Name   string `json:"name"`
	Topic  string `json:"topic"`
	Format string `json:"format"`
}
type Table struct {
	Name   string `json:"name"`
	Topic  string `json:"topic"`
	Format string `json:"format"`
}

type ListShowStreamsResponse []struct {
	Type          string   `json:"@type"`
	StatementText string   `json:"statementText"`
	Streams       []Stream `json:"streams"`
}

type ListShowTablesResponse []struct {
	Type          string  `json:"@type"`
	StatementText string  `json:"statementText"`
	Tables        []Table `json:"tables"`
}

type Query struct {
	QueryString string   `json:"queryString"`
	Sinks       []string `json:"sinks"`
	ID          string   `json:"id"`
}

type StreamsAndTablesSchema struct {
	Type         string                 `json:"type"`         // type (string) -- The type the schema represents. One of INTEGER, BIGINT, BOOLEAN, DOUBLE, STRING, MAP, ARRAY, or STRUCT.
	MemberSchema struct{}               `json:"memberSchema"` // memberSchema (object) -- A schema object. For MAP and ARRAY types, contains the schema of the map values and array elements, respectively. For other types this field is not used and its value is undefined.
	Fields       StreamsAndTablesFields `json:"fields"`       // fields (array) -- For STRUCT types, contains a list of field objects that descrbies each field within the struct. For other types this field is not used and its value is undefined.
}

type StreamsAndTablesFields []struct {
	Name   string                 `json:"name"`   // name   (string) -- The name of the field.
	Schema StreamsAndTablesSchema `json:"schema"` // schema (object) -- A schema object that describes the schema of the field.
}

type SourceDescription struct {
	Name         string                 `json:"name"`         // name         (string)  -- The name of the stream or table.
	ReadQueries  []Query                `json:"readQueries"`  // readQueries  (array)   -- The queries reading from the stream or table.
	WriteQueries []Query                `json:"writeQueries"` // writeQueries (array)   -- The queries writing into the stream or table
	Fields       StreamsAndTablesFields `json:"fields"`       // fields       (array)   -- A list of field objects that describes each field in the stream/table.
	Type         string                 `json:"type"`         // type         (string)  -- STREAM or TABLE
	key          string                 `json:"key"`          // key          (string)  -- The name of the key column.
	timestamp    string                 `json:"timestamp"`    // timestamp    (string)  -- The name of the timestamp column.
	format       string                 `json:"format"`       // format       (string)  -- The serialization format of the data in the stream or table. One of JSON, AVRO, or DELIMITED.
	topic        string                 `json:"topic"`        // topic        (string)  -- The topic backing the stream or table.
	extended     bool                   `json:"extended"`     // extended     (boolean) -- A boolean that indicates whether this is an extended description.
	statistics   string                 `json:"statistics"`   // statistics   (string)  -- A string that contains statistics about production and consumption to and from the backing topic (extended only).
	errorStats   string                 `json:"errorStats"`   // errorStats   (string)  -- A string that contains statistics about errors producing and consuming to and from the backing topic (extended only).
	replication  string                 `json:"replication"`  // replication  (int)     -- The replication factor of the backing topic (extended only).
	partitions   string                 `json:"partitions"`   // partitions   (int)     -- The number of partitions in the backing topic (extended only).
}

type DescribeResponse []struct {
	Type              string            `json:"@type"`
	StatementText     string            `json:"statementText"`
	SourceDescription SourceDescription `json:"sourceDescription"`
}
