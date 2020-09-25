package ksql

type ErrorMessage struct {
	Message    string   `json:"message"`
	StackTrace []string `json:"stackTrace"`
}

type QueryResponse struct {
	Row *struct {
		Columns []interface{} `json:"columns"`
	} `json:"row"`
	ErrorMessage ErrorMessage `json:"errorMessage"`
	FinalMessage ErrorMessage `json:"finalMessage"`
}

type Request struct {
	KSQL                 string            `json:"ksql"`
	StreamsProperties    map[string]string `json:"streamsProperties"`
	streamPropertiesName string
}

type Response []struct {
	Type          string `json:"@type"`
	StatementText string `json:"statementText"`

	// error responses
	StackTrace []string `json:"stackTrace"`
	ErrorCode  int      `json:"error_code"`
	Message    string   `json:"message"`
	// plus entities... list of something

	// response to various queries
	Streams   []Stream   `json:"streams"`
	Tables    []Table    `json:"tables"`
	Queries   []Query    `json:"queries"`
	Topics    []Topic    `json:"topics"`
	Functions []Function `json:"functions"`

	// responses to 'show properties'
	Properties            map[string]string `json:"properties"`
	OverwrittenProperties []string          `json:"overwrittenProperties"`
	DefaultProperties     []string          `json:"defaultProperties"`
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
	Name     string `json:"name"`
	Topic    string `json:"topic"`
	Format   string `json:"format"`
	Windowed bool   `json:"isWindowed"`
}

// Query is an item in a 'SHOW QUERIES' response
type Query struct {
	ID    string   `json:"id"`
	Sinks []string `json:"sinks"` // streams/tables, presumably
	KSQL  string   `json:"queryString"`
}

// Topic is an item in a 'SHOW TOPICS' response
type Topic struct {
	Name           string `json:"name"`
	Registered     bool   `json:"registered"`
	ReplicaInfo    []int  `json:"replicaInfo"`
	Consumers      int    `json:"consumerCount"`
	GroupConsumers int    `json:"consumerGroupCount"`
}

// Function is an item in a 'SHOW FUNCTIONS' response
type Function struct {
	Name string `json:"name"`
	Type string `json:"type"` // 'scalar' or 'aggregate'
}

type BasicAuth struct {
	Username string
	Password string
}
