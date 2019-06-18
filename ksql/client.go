package ksql

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"time"
)

// Logger provides a pluggable interface for caller-provided loggers
type Logger interface {
	Printf(string, ...interface{})
}

type KSQLServerInfo struct {
	Version        string `json:"version"`
	KafkaClusterID string `json:"kafkaClusterId"`
	KSQLServiceID  string `json:"ksqlServiceId"`
}

type InfoResponse struct {
	Info KSQLServerInfo `json:"KsqlServerInfo"`
}

const AcceptHeader = "application/vnd.ksql.v1+json"

// Client provides a client to interact with the KSQL REST API
type Client struct {
	context.Context
	client *http.Client
	host   string
	Logger
}

// NewClient returns a new client
func NewClient(host string) *Client {
	return NewClientContext(context.Background(), host)
}

// NewClientContext returns a new client which supports cancelation
// via the context
func NewClientContext(ctx context.Context, host string) *Client {
	c := &Client{
		Context: ctx,
		host:    host,
		client:  &http.Client{},
		Logger:  stdLogger{},
	}
	c.client.Transport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
	}
	return c
}

type stdLogger struct{}

// Printf implements the Logger interface
func (stdLogger) Printf(fmt string, args ...interface{}) {
	log.Printf(fmt, args...)
}

// CreateTable creates a KSQL Table
func (c *Client) CreateTable(req *CreateTableRequest) error {
	return c.qTOerr(req)
}

// CreateStream creates a KSQL Stream
func (c *Client) CreateStream(req *CreateStreamRequest) error {
	return c.qTOerr(req)
}

// DropTable drops a KSQL Table
func (c *Client) DropTable(req *DropTableRequest) error {
	cmdSeq, err := c.terminateBeforeDrop(req.Name)
	if err != nil {
		return err
	}
	return c.qTOerr(req, cmdSeq...)
}

// DropStream drops a KSQL Stream
func (c *Client) DropStream(req *DropStreamRequest) error {
	cmdSeq, err := c.terminateBeforeDrop(req.Name)
	if err != nil {
		return err
	}
	return c.qTOerr(req, cmdSeq...)
}

// Describe gets a KSQL Stream or Table
func (c *Client) Describe(name string) (*SourceDescription, error) {
	r := Request{
		KSQL: fmt.Sprintf("DESCRIBE %s;", name),
	}
	resp, err := c.ksqlRequest(r)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	res := DescribeResponse{}
	err = json.Unmarshal(body, &res)

	if err != nil {
		return nil, err
	}

	return &res[0].SourceDescription, nil
}

// ListStreams returns a slice of available streams
func (c *Client) ListStreams() ([]Stream, error) {
	r := Request{
		KSQL: "LIST STREAMS;",
	}
	resp, err := c.ksqlRequest(r)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	res := ListShowStreamsResponse{}
	err = json.Unmarshal(body, &res)

	if err != nil {
		return nil, err
	}

	return res[0].Streams, nil
}

// ListTables returns a slice of available tables
func (c *Client) ListTables() ([]Table, error) {
	r := Request{
		KSQL: "LIST Tables;",
	}

	resp, err := c.ksqlRequest(r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	res := ListShowTablesResponse{}
	err = json.Unmarshal(body, &res)

	if err != nil {
		return nil, err
	}

	return res[0].Tables, nil
}

// Terminate terminates the query
func (c *Client) Terminate(req *TerminateRequest, cmdSeq ...int) (*CurrentStatusResponse, error) {
	r := Request{
		KSQL: req.query(),
	}
	if len(cmdSeq) > 0 {
		r.CommandSequenceNumber = cmdSeq[len(cmdSeq)-1]
	}

	resp, err := c.Do(r)
	return resp[0].CurrentStatusResponse, err
}

// Info returns KSQL server info
func (c *Client) Info() (*KSQLServerInfo, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/info", c.host), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	log.Printf("[DEBUG] %s", string(body))
	s := &InfoResponse{}
	err = json.Unmarshal(body, s)

	return &s.Info, err
}

// Do provides a way for running queries against the `/ksql` endpoint
func (c *Client) Do(r Request) (ServerResponse, error) {
	res, err := c.ksqlRequest(r)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	log.Printf("[DEBUG] %s", string(body))

	if res.StatusCode >= 200 && res.StatusCode <= 299 {
		resp := ServerResponse{}
		err = json.Unmarshal(body, &resp)
		return resp, nil
	}

	errorResp := &ErrResp{}
	err = json.Unmarshal(body, errorResp)
	if err != nil {
		return nil, err
	}

	//if resp[0].ErrorCode != 0 {
	//return nil, errors.New(resp[0].Message + "\n" + strings.Join(resp[0].StackTrace, "\n"))
	//}

	return nil, errorResp
}

// Status provides a way to check the status of a previous command
func (c *Client) Status(commandID string) (*StatusResponse, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/status?commandID=%s", c.host, commandID), nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(c)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	s := &StatusResponse{}
	err = json.Unmarshal(body, s)
	return s, err
}

// Query runs a Request, parsing the response and sending each on the channel
func (c *Client) Query(r Request, ch chan *QueryResponse) error {
	return c.QueryContext(c, r, ch)
}

// QueryContext is a cancelable version of Query
func (c *Client) QueryContext(ctx context.Context, r Request, ch chan *QueryResponse) error {
	resp, err := c.doQueryContext(ctx, r)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	for {
		q, err := readQR(reader)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			c.Logger.Printf("error reading results from ksql query %#v: %s", r.KSQL, err)
			if err, ok := err.(net.Error); ok && !err.Temporary() {
				return err
			}
		}
		if q == nil {
			continue
		}
		select {
		case ch <- q:
		case <-ctx.Done():
			return nil
		}
	}
	return err
}

// LimitQuery runs a Request and returns the entire response
// Only use this with a query that contains a limit, or it will never return
func (c *Client) LimitQuery(r Request) ([]*QueryResponse, error) {
	resp, err := c.doQuery(r)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	qrs := []*QueryResponse{}
	for {
		q, err := readQR(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return qrs, err
		}
		if q == nil {
			continue
		}
		qrs = append(qrs, q)
	}

	return qrs, err
}

func (c *Client) doQuery(r Request) (*http.Response, error) {
	return c.doQueryContext(c, r)
}

func (c *Client) doQueryContext(ctx context.Context, r Request) (*http.Response, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/query", c.host), bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	req.Header.Set("Accept", AcceptHeader)
	req.Header.Set("Content-Type", "application/json")

	return c.client.Do(req)
}

func readQR(rd *bufio.Reader) (*QueryResponse, error) {
	line, err := rd.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	sLine := strings.TrimSpace(string(line))
	if len(sLine) == 0 {
		return nil, nil
	}
	q := &QueryResponse{}
	err = json.Unmarshal(line, q)
	return q, err
}

func (c *Client) ksqlRequest(r Request) (*http.Response, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/ksql", c.host), bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}
	req = req.WithContext(c)

	req.Header.Set("Accept", AcceptHeader)
	req.Header.Set("Content-Type", "application/json")
	return c.client.Do(req)
}

func (c *Client) qTOerr(req queryRequest, commandSequence ...int) error {
	r := Request{
		KSQL: req.query(),
	}

	if len(commandSequence) > 0 {
		r.CommandSequenceNumber = commandSequence[len(commandSequence)-1]
	}

	res, err := c.Do(r)
	log.Printf("[DEBUG] %v", res)

	return err
}

type queryRequest interface {
	query() string
}

func (c *Client) terminateBeforeDrop(name string) ([]int, error) {
	log.Printf("[LOG] Terminating queries for '%s'", name)

	desc, err := c.Describe(name)
	commandSequence := make([]int, len(desc.WriteQueries))

	if err != nil {
		return commandSequence, err
	}

	if len(desc.ReadQueries) > 0 {
		dependency := desc.ReadQueries[0].Sinks[0]
		return commandSequence, fmt.Errorf("could not drop '%s', '%s' needs to be dropped before", name, dependency)
	}

	for _, q := range desc.WriteQueries {
		expectedSinks := []string{strings.ToUpper(name)}
		if !reflect.DeepEqual(q.Sinks, expectedSinks) {
			return commandSequence, fmt.Errorf("could not drop '%s', the query '%s' should sinks '%v' but '%v' was found instead", name, q.ID, expectedSinks, q.Sinks)
		}
		status, err := c.Terminate(&TerminateRequest{Name: q.ID}, commandSequence...)
		if err != nil {
			return commandSequence, err
		}
		commandSequence = append(commandSequence, status.CommandSequenceNumber)
	}

	log.Printf("command sequence: %v", commandSequence)

	return commandSequence, nil
}
