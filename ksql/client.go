package ksql

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

// Logger provides a pluggable interface for caller-provided loggers
type Logger interface {
	Printf(string, ...interface{})
}

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
	return resp[0].Streams, nil
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
	return resp[0].Tables, nil
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

	if resp[0].ErrorCode != 0 {
		return nil, errors.New(resp[0].Message + "\n" + strings.Join(resp[0].StackTrace, "\n"))
	}
	return resp, nil
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

	req.Header.Set("Content-Type", "application/json")
	return c.client.Do(req)
}
