package client

import (
	"context"
)

// Stream is info about a stream
type Stream struct {
	// Name is the name of the stream
	Name string `json:"name"`
	// Topic is the associated Kafka topic
	Topic string `json:"topic"`
	// Format is the serialization format of the stream. One of JSON, AVRO, PROTOBUF, or DELIMITED.
	Format string `json:"format"`
	// Type is always 'STREAM'
	Type string `json:"type"`
}

// ListStreamsResult represents the API response from the `LIST STREAMS;` operation
type ListStreamsResult struct {
	commonResult
	// Streams is the list of streams returned
	Streams []Stream `json:"streams,omitempty"`
}

func (ls *ListStreamsResult) is(target ExecResult) bool {
	if target.ListStreamsResult != nil {
		*ls = *target.ListStreamsResult
		ls.commonResult = target.commonResult
		return true
	}
	return false
}

// ListStreams is a convenience method which executes a `LIST STREAMS;` operation
func (c *ksqldb) ListStreams(ctx context.Context) (ListStreamsResult, error) {
	var ls ListStreamsResult
	res, err := c.singleExec(ctx, ExecPayload{KSQL: "LIST STREAMS;"})
	if err != nil {
		return ls, err
	}
	_ = res.As(&ls)
	return ls, nil
}

// Table is info about a table
type Table struct {
	// Name of the table.
	Name string `json:"name"`
	// Topic backing the table.
	Topic string `json:"topic"`
	// The serialization format of the data in the table. One of JSON, AVRO, PROTOBUF, or DELIMITED.
	Format string `json:"format"`
	// The source type. Always returns 'TABLE'.
	Type string `json:"type"`
	// IsWindowed is true if the table provides windowed results; otherwise, false.
	IsWindowed bool `json:"isWindowed"`
}

// ListTablesResult represents the API response from the `LIST TABLES;` operation
type ListTablesResult struct {
	commonResult
	// Tables is the list of tables returned
	Tables []Table `json:"tables,omitempty"`
}

func (lt *ListTablesResult) is(target ExecResult) bool {
	if target.ListTablesResult != nil {
		*lt = *target.ListTablesResult
		lt.commonResult = target.commonResult
		return true
	}
	return false
}

// ListTables is a convenience method which executes a `LIST TABLES;` operation
func (c *ksqldb) ListTables(ctx context.Context) (ListTablesResult, error) {
	var lt ListTablesResult
	res, err := c.singleExec(ctx, ExecPayload{KSQL: "LIST TABLES;"})
	if err != nil {
		return lt, err
	}
	_ = res.As(&lt)
	return lt, nil
}

// ListQueriesResult represents the API response from the `LIST QUERIES;` operation
type ListQueriesResult struct {
	commonResult
	// Queries is the list of running queries
	Queries []Query `json:"queries,omitempty"`
}

func (lq *ListQueriesResult) is(target ExecResult) bool {
	if target.ListQueriesResult != nil {
		*lq = *target.ListQueriesResult
		lq.commonResult = target.commonResult
		return true
	}
	return false
}

// ListQueries is a convenience method which executes a `LIST QUERIES;` operation
func (c *ksqldb) ListQueries(ctx context.Context) (ListQueriesResult, error) {
	var lq ListQueriesResult
	res, err := c.singleExec(ctx, ExecPayload{KSQL: "LIST QUERIES;"})
	if err != nil {
		return lq, err
	}
	_ = res.As(&lq)
	return lq, nil
}

// ListPropertiesResult represents the response for a `LIST PROPERTIES;` statement
type ListPropertiesResult struct {
	commonResult
	// Properties is the map of server query properties
	Properties map[string]string `json:"properties,omitempty"`
}

func (lp *ListPropertiesResult) is(target ExecResult) bool {
	if target.ListPropertiesResult != nil {
		*lp = *target.ListPropertiesResult
		lp.commonResult = target.commonResult
		return true
	}
	return false
}

func (c *ksqldb) ListProperties(ctx context.Context) (ListPropertiesResult, error) {
	var lp ListPropertiesResult
	res, err := c.singleExec(ctx, ExecPayload{KSQL: "LIST PROPERTIES;"})
	if err != nil {
		return lp, err
	}
	_ = res.As(&lp)
	return lp, nil
}
