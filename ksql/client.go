package ksql

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

// Client provides a client to interact with the KSQL REST API
type Client struct {
	client *http.Client
	host   string
}

type queryRequest interface {
	query() string
}

// NewClient returns a new client
func NewClient(host string) *Client {
	return &Client{
		host:   host,
		client: &http.Client{},
	}
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
	return c.qTOerr(req)
}

// DropStream drops a KSQL Stream
func (c *Client) DropStream(req *DropStreamRequest) error {
	return c.qTOerr(req)
}

// ListStreams returns a slice of available streams
func (c *Client) ListStreams() ([]Stream, error) {
	r := Request{
		KSQL: "LIST STREAMS;",
	}
	resp, err := c.Do(r)
	if err != nil {
		return nil, err
	}
	if len(resp) < 1 {
		return nil, errors.New("Didn't get enough responses")
	}
	return resp[0].Streams.Streams, nil
}

// ListTables returns a slice of available tables
func (c *Client) ListTables() ([]Table, error) {
	r := Request{
		KSQL: "LIST Tables;",
	}
	resp, err := c.Do(r)
	if err != nil {
		return nil, err
	}
	if len(resp) < 1 {
		return nil, errors.New("Didn't get enough responses")
	}
	return resp[0].Tables.Tables, nil
}

// Do provides a way for running queries against the `/ksql` endpoint
func (c *Client) Do(r Request) (Response, error) {
	res, err := c.ksqlRequest(r)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	resp := Response{}
	err = json.Unmarshal(body, &resp)

	if err != nil {
		return nil, err
	}

	if resp[0].Error != nil {
		return nil, errors.New(resp[0].Error.ErrorMessage.Message + "\n" + strings.Join(resp[0].Error.ErrorMessage.StackTrace, "\n"))
	}
	return resp, nil
}

// Status provides a way to check the status of a previous command
func (c *Client) Status(commandID string) (*StatusResponse, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/status?commandID=%s", c.host, commandID), nil)
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
	s := &StatusResponse{}
	err = json.Unmarshal(body, s)
	return s, err
}

// Query runs a Request, parsing the response and sending each on the channel
func (c *Client) Query(r Request, ch chan *QueryResponse) error {
	resp, err := c.doQuery(r)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	for {
		q, err := readQR(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println(err)
		}
		if q == nil {
			continue
		}
		ch <- q
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
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/query", c.host), bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}

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

	req.Header.Set("Content-Type", "application/json")
	return c.client.Do(req)
}

func (c *Client) qTOerr(req queryRequest) error {
	r := Request{
		KSQL: req.query(),
	}

	res, err := c.Do(r)
	log.Printf("[DEBUG] %v", res)

	return err
}
