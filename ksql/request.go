package ksql

import "log"

// Request represents a KSQL REST API request.
type Request struct {
	CommandSequenceNumber int               `json:"commandSequenceNumber,omitempty"`
	KSQL                  string            `json:"ksql"`
	StreamsProperties     map[string]string `json:"streamsProperties,omitempty"`
	streamPropertiesName  string
}

// CoordinateInSequence modifies the request to tell the server that a previous command in the sequence.
// See https://docs.confluent.io/current/ksql/docs/developer-guide/api.html#coordinate-multiple-requests.
func (r *Request) CoordinateInSequence(commandSequence ...int) {
	if len(commandSequence) > 0 {
		r.CommandSequenceNumber = commandSequence[len(commandSequence)-1]
		log.Printf("[DEBUG] coordinate request to run after command %d", r.CommandSequenceNumber)
	}
}
