package ksql

import (
	"fmt"
	"strings"
)

// ErrorMessage represents an error message with its stack trace.
type ErrorMessage struct {
	Message    string   `json:"message"`
	StackTrace []string `json:"stackTrace"`
}

// QueryResponse represents a KSQL REST API query response.
type QueryResponse struct {
	Row *struct {
		Columns []interface{} `json:"columns"`
	} `json:"row"`
	ErrorMessage ErrorMessage `json:"errorMessage"`
}

// Request represents a KSQL REST API request.
type Request struct {
	KSQL                 string            `json:"ksql"`
	StreamsProperties    map[string]string `json:"streamsProperties,omitempty"`
	streamPropertiesName string
}

// Response represents the KSQL REST API response to any request.
type Response []IntResponse

// ErrResp represents the KSQL REST API error response to any request.
type ErrResp struct {
	Type         string   `json:"@type,omitempty"`
	ErrorCode    int      `json:"error_code,omitempty"`
	ErrorMessage string   `json:"message,omitempty"`
	StackTrace   []string `json:"stackTrace,omitempty"`
}

// Error returns a formated error message.
func (e *ErrResp) Error() string {
	stackTrace := strings.Join(e.StackTrace, "\n")
	return fmt.Sprintf("%d [%s]: %s\n StackTrace:\n %s", e.ErrorCode, e.Type, e.ErrorMessage, stackTrace)
}

// IntResponse represents the KSQL REST API available responses structure.
type IntResponse struct {
	// DESCRIBE
	Description *struct {
		StatementText     string            `json:"statementText"`
		SourceDescription SourceDescription `json:"sourceDescription"`
	} `json:"sourceDescription,omitempty"`

	// Errors
	Error *struct {
		StatementText string       `json:"statementText"`
		ErrorMessage  ErrorMessage `json:"errorMessage"`
	} `json:"error,omitempty"`

	// LIST STREAMS, SHOW STREAMS
	Streams *struct {
		StatementText string   `json:"statementText"`
		Streams       []Stream `json:"streams"`
	} `json:"streams,omitempty"`

	// LIST TABLES, SHOW TABLES
	Tables *struct {
		StatementText string  `json:"statementText"`
		Tables        []Table `json:"tables"`
	} `json:"tables,omitempty"`

	// CREATE, DROP, TERMINATE
	Status *struct {
		StatementText string `json:"statementText"`
		CommandID     string `json:"commandId"`
		CommandStatus *struct {
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"commandStatus"`
	} `json:"currentStatus"`
}

// StatusResponse represents the KSQL REST API response status.
type StatusResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// Stream represents a KSQL "Stream". See https://docs.confluent.io/current/ksql/docs/developer-guide/syntax-reference.html#stream.
type Stream struct {
	Name   string `json:"name"`
	Topic  string `json:"topic"`
	Format string `json:"format"`
}

// Table represents a KSQL "Table". See https://docs.confluent.io/current/ksql/docs/developer-guide/syntax-reference.html#table
type Table struct {
	Name   string `json:"name"`
	Topic  string `json:"topic"`
	Format string `json:"format"`
}

// ListShowStreamsResponse represents the KSQL REST API response of a "LIST"/"SHOW" for "STREAMS" request.
// See https://docs.confluent.io/current/ksql/docs/developer-guide/api.html#run-a-ksql-statement
// for KSQL REST API reference.
type ListShowStreamsResponse []struct {
	Type          string   `json:"@type"`
	StatementText string   `json:"statementText"`
	Streams       []Stream `json:"streams"`
}

// ListShowTablesResponse represents the KSQL REST API response of a "LIST"/"SHOW" for "TABLES" query.
// See https://docs.confluent.io/current/ksql/docs/developer-guide/api.html#run-a-ksql-statement
// for KSQL REST API reference.
type ListShowTablesResponse []struct {
	Type          string  `json:"@type"`
	StatementText string  `json:"statementText"`
	Tables        []Table `json:"tables"`
}

// StreamsAndTablesSchema represents the schema attribute of a stream or a table field (StreamsAndTablesFields).
type StreamsAndTablesSchema struct {
	Type         string                 `json:"type"`         // The type the schema represents. One of INTEGER, BIGINT, BOOLEAN, DOUBLE, STRING, MAP, ARRAY, or STRUCT.
	MemberSchema struct{}               `json:"memberSchema"` // A schema object. For MAP and ARRAY types, contains the schema of the map values and array elements, respectively. For other types this field is not used and its value is undefined.
	Fields       StreamsAndTablesFields `json:"fields"`       // For STRUCT types, contains a list of field objects that describes each field within the struct. For other types this field is not used and its value is undefined.
}

// StreamsAndTablesFields represents the field attributes of a stream or a table.
type StreamsAndTablesFields []struct {
	Name   string                 `json:"name"`   // The name of the field.
	Schema StreamsAndTablesSchema `json:"schema"` // A schema object that describes the schema of the field.
}

// Query represents a KSQL "Query".
type Query struct {
	QueryString string   `json:"queryString"`
	Sinks       []string `json:"sinks"`
	ID          string   `json:"id"`
}

// SourceDescription represents the KSQL REST API "sourceDescription" field of response of a "DESCRIBE" query (DescribeResponse).
// See https://docs.confluent.io/current/ksql/docs/developer-guide/api.html#run-a-ksql-statement
// for KSQL REST API reference.
type SourceDescription struct {
	Name         string                 `json:"name"`         // The name of the stream or table.
	ReadQueries  []Query                `json:"readQueries"`  // The queries reading from the stream or table.
	WriteQueries []Query                `json:"writeQueries"` // The queries writing into the stream or table
	Fields       StreamsAndTablesFields `json:"fields"`       // A list of field objects that describes each field in the stream/table.
	Type         string                 `json:"type"`         // STREAM or TABLE
	Key          string                 `json:"key"`          // The name of the key column.
	Timestamp    string                 `json:"timestamp"`    // The name of the timestamp column.
	Format       string                 `json:"format"`       // The serialization format of the data in the stream or table. One of JSON, AVRO, or DELIMITED.
	Topic        string                 `json:"topic"`        // The topic backing the stream or table.
	Extended     bool                   `json:"extended"`     // A boolean that indicates whether this is an extended description.
	Statistics   string                 `json:"statistics"`   // A string that contains statistics about production and consumption to and from the backing topic (extended only).
	ErrorStats   string                 `json:"errorStats"`   // A string that contains statistics about errors producing and consuming to and from the backing topic (extended only).
	Replication  int                    `json:"replication"`  // The replication factor of the backing topic (extended only).
	Partitions   int                    `json:"partitions"`   // The number of partitions in the backing topic (extended only).
}

// DescribeResponse represents the KSQL REST API response of a "DESCRIBE" query.
// See https://docs.confluent.io/current/ksql/docs/developer-guide/api.html#run-a-ksql-statement
// for KSQL REST API reference.
type DescribeResponse []struct {
	Type              string            `json:"@type"`
	StatementText     string            `json:"statementText"`
	SourceDescription SourceDescription `json:"sourceDescription"`
}
