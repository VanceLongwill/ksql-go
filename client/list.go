package ksql

import (
	"context"
	"errors"
)

func (c *Client) doListExec(ctx context.Context, payload ExecPayload) (ExecResult, error) {
	var resp ExecResult
	results, err := c.Exec(ctx, payload)
	if err != nil {
		return resp, err
	}
	if len(results) == 0 {
		return resp, errors.New("Unexpected empty response list from Exec")
	}
	return results[0], nil
}

// ListStreamsResult represents the API response from the `LIST STREAMS;` operation
type ListStreamsResult struct {
	commonResult
	// Streams is the list of streams returned
	Streams []Stream `json:"streams,omitempty"`
}

// ListStreams is a convenience method which executes a `LIST STREAMS;` operation
func (c *Client) ListStreams(ctx context.Context) (*ListStreamsResult, error) {
	res, err := c.doListExec(ctx, ExecPayload{KSQL: "LIST STREAMS;"})
	if err != nil {
		return nil, err
	}
	return &ListStreamsResult{
		Streams: res.Streams,
	}, nil
}

// ListTablesResult represents the API response from the `LIST TABLES;` operation
type ListTablesResult struct {
	commonResult
	// Tables is the list of tables returned
	Tables []Table `json:"tables,omitempty"`
}

// ListTables is a convenience method which executes a `LIST TABLES;` operation
func (c *Client) ListTables(ctx context.Context) (*ListTablesResult, error) {
	res, err := c.doListExec(ctx, ExecPayload{KSQL: "LIST TABLES;"})
	if err != nil {
		return nil, err
	}
	return &ListTablesResult{
		commonResult: res.commonResult,
		Tables:       res.Tables,
	}, nil
}

// ListQueriesResult represents the API response from the `LIST QUERIES;` operation
type ListQueriesResult struct {
	commonResult
	// Queries is the list of running queries
	Queries []Query `json:"tables,omitempty"`
}

// ListQueries is a convenience method which executes a `LIST QUERIES;` operation
func (c *Client) ListQueries(ctx context.Context) (*ListQueriesResult, error) {
	res, err := c.doListExec(ctx, ExecPayload{KSQL: "LIST QUERIES;"})
	if err != nil {
		return nil, err
	}
	return &ListQueriesResult{
		commonResult: res.commonResult,
		Queries:      res.Queries,
	}, nil
}
