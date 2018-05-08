package ksql

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type Client struct {
	client *http.Client
	host   string
}

func NewClient(host string) *Client {
	return &Client{
		host:   host,
		client: &http.Client{},
	}
}

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
func (c *Client) Status(commandID string) (*StatusResponse, error) {
	req, err := http.NewRequest("GET", c.host+"/status?commandID="+commandID, nil)
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

func (c *Client) Query(r Request, ch chan *QueryResponse) error {
	resp, err := c.DoQuery(r)
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

func (c *Client) LimitQuery(r Request) ([]*QueryResponse, error) {
	resp, err := c.DoQuery(r)
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

func (c *Client) DoQuery(r Request) (*http.Response, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", c.host+"/query", bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	return c.client.Do(req)
}
func (c *Client) ksqlRequest(r Request) (*http.Response, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", c.host+"/ksql", bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	return c.client.Do(req)
}
