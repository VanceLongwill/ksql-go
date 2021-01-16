package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// QueryResultHeader is a header object which contains details of the push & pull query results
type QueryResultHeader struct {
	// QueryID is a unique ID, provided for push queries only
	QueryID string `json:"queryID"`
	// ColumnNames is a list of column names
	ColumnNames []string `json:"columnNames"`
	// ColumnTypes is a list of the column types (e.g. 'BIGINT', 'STRING', 'BOOLEAN')
	ColumnTypes []string `json:"columnTypes"`
}

// QueryStreamPayload is the request body type for the /query-stream endpoint
type QueryStreamPayload struct {
	// KSQL is the SELECT query to execute
	KSQL string `json:"sql"`
	// Properties is a map of optional properties for the query
	Properties map[string]string `json:"properties,omitempty"`
}

type queryStreamReadCloser struct {
	queryID string
	body    io.ReadCloser
	client  *Client
}

func (q *queryStreamReadCloser) Read(b []byte) (int, error) {
	return q.body.Read(b)
}

func (q *queryStreamReadCloser) Close() error {
	if err := q.client.CloseQuery(context.Background(), CloseQueryPayload{q.queryID}); err != nil {
		return err
	}
	return q.body.Close()
}

// QueryStream runs a streaming push & pull query
func (c *Client) QueryStream(ctx context.Context, payload QueryStreamPayload) (*QueryStreamRows, error) {
	b := &bytes.Buffer{}
	err := json.NewEncoder(b).Encode(&payload)
	if err != nil {
		return nil, err
	}
	req, err := makeRequest(ctx, c.baseURL, queryStreamPath, http.MethodPost, b)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("unable to get response: %w", err)
	}
	dec := json.NewDecoder(resp.Body)
	var header QueryResultHeader
	if err := dec.Decode(&header); err != nil {
		return nil, err
	}
	r := &QueryStreamRows{
		ctx: ctx,
		body: &queryStreamReadCloser{
			queryID: header.QueryID,
			body:    resp.Body,
			client:  c,
		},
		dec: dec,
		columns: columns{
			count: len(header.ColumnNames),
			names: header.ColumnNames,
		},
	}
	c.rows = append(c.rows, r)
	return r, nil
}

// CloseQueryPayload represents the JSON body used to close a query stream
type CloseQueryPayload struct {
	QueryID string `json:"queryId"`
}

// CloseQuery explicitly terminates a push query stream
func (c *Client) CloseQuery(ctx context.Context, payload CloseQueryPayload) error {
	b := &bytes.Buffer{}
	if err := json.NewEncoder(b).Encode(&payload); err != nil {
		return err
	}
	req, err := makeRequest(ctx, c.baseURL, closeQueryPath, http.MethodPost, b)
	if err != nil {
		return err
	}
	if _, err := c.http.Do(req); err != nil {
		return err
	}
	return nil
}
