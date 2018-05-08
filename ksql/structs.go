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
