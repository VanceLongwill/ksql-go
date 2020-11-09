package ksql

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	// "golang.org/x/sync/errgroup"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"path"

	"golang.org/x/net/http2"
)

var (
	// AcceptJSON is the http accept header for requesting JSON responses
	AcceptJSON = "application/vnd.ksql.v1+json"
	// AcceptDelimited is the http accept header for requesting DELIMITED responses (default for push & pull queries)
	AcceptDelimited = "application/vnd.ksqlapi.delimited.v1"
)

var (
	QueryPath            = "/query"
	ExecPath             = "/ksql"
	QueryStreamPath      = "/query-stream"
	CloseQueryPath       = "/close-query"
	InsertsStreamPath    = "/inserts-stream"
	TerminateClusterPath = "/ksql/terminate"
	InfoPath             = "/info"
	HealthCheckPath      = "/healthcheck"
)

// Client is a ksqlDB client
type Client struct {
	client               *http.Client
	http2                *http.Client
	baseURL              string
	rows                 []*Rows
	insertsStreamWriters []*InsertsStreamWriter
}

func (c Client) makeRequest(ctx context.Context, urlPath string, method string, rdr io.Reader) (*http.Request, error) {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, urlPath)
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rdr)
	if err != nil {
		return nil, err
	}
	switch urlPath {
	case QueryStreamPath, InsertsStreamPath:
		req.Header.Add("Accept", AcceptDelimited)
		req.Header.Add("Content-Type", AcceptDelimited)
	default:
		req.Header.Add("Accept", "application/vnd.ksql.v1+json")
		req.Header.Add("Content-Type", "application/vnd.ksql.v1+json")
	}
	return req, nil

}

// QueryPayload represents the JSON payload for the POST /query endpoint
type QueryPayload struct {
	// KSQL is SELECT statement
	KSQL string `json:"ksql"`
	// StreamsProperties is a map of property overrides
	StreamsProperties StreamsProperties `json:"streamsProperties,omitempty"`
}

// Row is a row in the DB
type Row struct {
	Columns []driver.Value `json:"columns"`
}

// QueryResult is the result of running a query
type QueryResult struct {
	Row          Row    `json:"row"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	FinalMessage string `json:"finalMessage,omitempty"`
}

type QueryError struct {
	result map[string]interface{}
}

func (q *QueryError) Error() string {
	if msg, ok := q.result["message"]; ok {
		return msg.(string)
	}
	return "an unknown error occurred"
}

type QueryRows struct {
	res    []map[string]interface{}
	i      int
	closed bool
	columns
}

func (q *QueryRows) Next(dest []driver.Value) error {
	if q.closed {
		return ErrRowsClosed
	}
	if q.i > len(q.res)-1 {
		return io.EOF
	}
	row, exists := q.res[q.i]["row"]
	if !exists {
		return errors.New("unable to get row object")
	}
	rowMap, ok := row.(map[string]interface{})
	if !ok {
		return errors.New("row object has incorrect type")
	}
	cols, ok := rowMap["columns"]
	if !ok {
		return errors.New("unable to get columns from row object")
	}
	colsSlice, ok := cols.([]interface{})
	if !ok {
		return errors.New("unable to convert columns to slice")
	}
	if err := q.columns.Validate(dest); err != nil {
		return err
	}
	for idx, v := range colsSlice {
		dest[idx] = v.(driver.Value)
	}
	q.i++
	return nil
}

func (q *QueryRows) Close() error {
	q.closed = true
	return nil
}

// Query runs a KSQL query and returns a cursor. For streaming results use the QueryStream method.
func (c Client) Query(ctx context.Context, payload QueryPayload) (*QueryRows, error) {
	b := &bytes.Buffer{}
	err := json.NewEncoder(b).Encode(&payload)
	if err != nil {
		return nil, err
	}
	req, err := c.makeRequest(ctx, QueryPath, http.MethodPost, b)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("unable to get response: %w", err)
	}
	defer resp.Body.Close()
	by, err := ioutil.ReadAll(resp.Body)
	var statementError map[string]interface{}
	if err := json.Unmarshal(by, &statementError); err == nil {
		return nil, &QueryError{statementError}
	}
	var resultsRaw []map[string]interface{}
	if err := json.Unmarshal(by, &resultsRaw); err != nil {
		return nil, err
	}
	cols := columns{
		count: -1,
	}
	if h, ok := resultsRaw[0]["header"]; ok {
		if headerMap, ok := h.(map[string]interface{}); ok {
			if schema, exists := headerMap["schema"]; exists {
				cols.names = parseSchemaKeys(schema.(string))
				cols.count = len(cols.names)
			}
		}
	}
	return &QueryRows{
		res:     resultsRaw[1:],
		columns: cols,
	}, nil
}

// ExecPayload represents the JSON payload for the /ksql endpoint
type ExecPayload struct {
	// KSQL is a sequence of SQL statements. Anything is permitted except SELECT, for which you should use the Query method
	KSQL string `json:"ksql"`
	// StreamsProperties is a map of property overrides
	StreamsProperties StreamsProperties `json:"streamsProperties,omitempty"`
	// CommandSequenceNumber optionally waits until the specified sequence has been completed before running
	CommandSequenceNumber int64 `json:"commandSequenceNumber,omitempty"`
}

// Warning represents a non-fatal user warning
type Warning struct {
	Message string `json:"message"`
}

// CommandStatus contains details of status of a given command
type CommandStatus struct {
	// Status is one of QUEUED, PARSING, EXECUTING, TERMINATED, SUCCESS, or ERROR
	Status string `json:"status"`
	// Message regarding the status of the execution statement.
	Message string `json:"message"`
	// CommandSequenceNumber is the sequence number of the command, -1 if unsuccessful
	CommandSequenceNumber int64 `json:"commandSequenceNumber"`
}

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

// Query is info about a query
type Query struct {
	// QueryString is the text of the statement that started the query
	QueryString string `json:"queryString"`
	// Sinks are the streams and tables being written to by the query
	Sinks string `json:"sinks"`
	// ID is the query id
	ID string `json:"id"`
}

// Schema represents a ksqlDB fields schema
type Schema struct {
	// The type the schema represents. One of INTEGER, BIGINT, BOOLEAN, DOUBLE, STRING, MAP, ARRAY, or STRUCT.
	Type string `json:"type"`
	// A schema object. For MAP and ARRAY types, contains the schema of the map values and array elements, respectively. For other types this field is not used and its value is undefined.
	MemberSchema map[string]interface{} `json:"memberSchema,omitempty"`
	// For STRUCT types, contains a list of field objects that describes each field within the struct. For other types this field is not used and its value is undefined.
	Fields []Field `json:"fields,omitempty"`
}

// Field represents a single fields in ksqlDB
type Field struct {
	// The name of the field.
	Name string `json:"name"`
	// A schema object that describes the schema of the field.
	Schema Schema `json:"schema"`
}

// SourceDescription is a detailed description of the source (a STREAM or TABLE)
type SourceDescription struct {
	// Name of the stream or table.
	Name string `json:"name"`
	// ReadQueries is the list of queries reading from the stream or table.
	ReadQueries []Query `json:"readQueries"`
	// WriteQueries is the list of queries writing into the stream or table
	WriteQueries []Query `json:"writeQueries"`
	// Fields is a list of field objects that describes each field in the stream/table.
	Fields []Field `json:"fields"`
	// Type is either STREAM or TABLE.
	Type string `json:"type"`
	// Key is the name of the key column.
	Key string `json:"key"`
	// Timestamp is the name of the timestamp column.
	Timestamp string `json:"timestamp"`
	// Format is the serialization format of the data in the stream or table. One of JSON, AVRO, PROTOBUF, or DELIMITED.
	Format string `json:"format"`
	// Topic backing the stream or table.
	Topic string `json:"topic"`
	// Extended indicates if this is an extended description.
	Extended bool `json:"extended"`
	// Statistics about production and consumption to and from the backing topic (extended only).
	Statistics string `json:"statistics,omitempty"`
	// ErrorStats is a string about errors producing and consuming to and from the backing topic (extended only).
	ErrorStats string `json:"errorStats,omitempty"`
	// Replication factor of the backing topic (extended only).
	Replication int `json:"replication,omitempty"`
	// Partitions is the number of partitions in the backing topic (extended only).
	Partitions int `json:"partitions,omitempty"`
}

// QueryDescription is a detailed description of a query statement.
type QueryDescription struct {
	// StatementText is a ksqlDB statement for which the query being explained is running.
	StatementText string `json:"statementText"`
	// Fields is a list of field objects that describes each field in the query output.
	Fields []Field `json:"fields"`
	// Sources is a list of the stream and table names being read by the query.
	Sources []string `json:"sources"`
	// Sinks is a list of the stream and table names being written to by the query.
	Sinks []string `json:"sinks"`
	// ExecutionPlan is the query execution plan.
	ExecutionPlan string `json:"executionPlan"`
	// Topology is the Kafka Streams topology for the query that is running.
	Topology string `json:"topology"`
}

// ExecResult is the response result from the /ksql endpoint
type ExecResult struct {
	// Common to all responses

	// StatementText is the text of the SQL statement where the error occurred
	StatementText string `json:"statementText"`
	// A list of non-fatal warning messages
	Warnings []Warning `json:"warnings"`

	// CREATE, DROP, TERMINATE

	// CommandID is the identified for the requested operation. You can use this ID to poll the result of the operation using the status endpoint.
	CommandID string `json:"commandId,omitempty"`
	// CommandStatus is the status of the requested operation.
	CommandStatus CommandStatus `json:"commandStatus,omitempty"`

	// LIST STREAMS, SHOW STREAMS

	// Streams is the list of streams returned
	Streams []Stream `json:"streams,omitempty"`

	// LIST TABLES, SHOW TABLES

	// Tables is the list of tables returned
	Tables []Table `json:"tables,omitempty"`

	// LIST QUERIES, SHOW QUERIES

	// Queries is the list of queries started
	Queries []Query `json:"queries,omitempty"`

	// LIST PROPERTIES, SHOW PROPERTIES

	// Properties is the map of server query properties
	Properties map[string]string `json:"properties,omitempty"`

	// DESCRIBE

	// SourceDescription is a detailed description of the source (a STREAM or TABLE)
	SourceDescription SourceDescription `json:"sourceDescription,omitempty"`

	// EXPLAIN

	// QueryDescription is a detailed description of a query statement.
	QueryDescription QueryDescription `json:"queryDescription,omitempty"`
	// OverriddenProperties is a map of property overrides that the query is running with.
	OverriddenProperties map[string]interface{} `json:"overriddenProperties,omitempty"`
}

// Exec runs KSQL statements which can be anything except SELECT
func (c Client) Exec(ctx context.Context, payload ExecPayload) ([]ExecResult, error) {
	b := &bytes.Buffer{}
	err := json.NewEncoder(b).Encode(&payload)
	if err != nil {
		return nil, err
	}
	req, err := c.makeRequest(ctx, ExecPath, http.MethodPost, b)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("unable to make Exec request: %w", err)
	}
	var results []ExecResult
	by, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("unable to read response body: %w", err)
	}
	if err := json.Unmarshal(by, &results); err != nil {
		var result ExecResult
		if err := json.Unmarshal(by, &result); err != nil {
			return nil, fmt.Errorf("unable to decode JSON response '%s': %w", string(by), err)
		}
		results = append(results, result)
	}
	return results, nil
}

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
	client  Client
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
func (c Client) QueryStream(ctx context.Context, payload QueryStreamPayload) (*Rows, error) {
	b := &bytes.Buffer{}
	err := json.NewEncoder(b).Encode(&payload)
	if err != nil {
		return nil, err
	}
	req, err := c.makeRequest(ctx, QueryStreamPath, http.MethodPost, b)
	if err != nil {
		return nil, err
	}
	resp, err := c.http2.Do(req)
	if err != nil {
		return nil, fmt.Errorf("unable to get response: %w", err)
	}
	dec := json.NewDecoder(resp.Body)
	var header QueryResultHeader
	if err := dec.Decode(&header); err != nil {
		return nil, err
	}
	r := &Rows{
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
func (c Client) CloseQuery(ctx context.Context, payload CloseQueryPayload) error {
	b := &bytes.Buffer{}
	if err := json.NewEncoder(b).Encode(&payload); err != nil {
		return err
	}
	req, err := c.makeRequest(ctx, CloseQueryPath, http.MethodPost, b)
	if err != nil {
		return err
	}
	if _, err := c.client.Do(req); err != nil {
		return err
	}
	return nil
}

type InsertsStreamTargetPayload struct {
	Target string `json:"target"`
}

type InsertsStreamAck struct {
	Status string `json:"status"`
	Seq    int64  `json:"seq"`
}

var ErrAckUnsucessful = errors.New("an ack was received but the status was not 'ok'")

// InsertsStream llows you to insert rows into an existing ksqlDB stream. The stream must have already been created in ksqlDB.
func (c Client) InsertsStream(ctx context.Context, payload InsertsStreamTargetPayload) (*InsertsStreamWriter, error) {
	pr, pw := io.Pipe()
	req, err := c.makeRequest(ctx, InsertsStreamPath, http.MethodPost, ioutil.NopCloser(pr))
	if err != nil {
		return nil, err
	}
	rdr := &bytes.Buffer{}

	// g, ctx := errgroup.New()

	go func() {
		if err := json.NewEncoder(pw).Encode(&payload); err != nil {
			log.Fatalln(err)
		}
	}()

	go func() {
		resp, err := c.http2.Do(req)
		if err != nil {
			log.Fatalln(err)
		}
		res, err := io.Copy(rdr, resp.Body)
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("Got: %#v", res)
		var ack InsertsStreamAck
		err = json.NewDecoder(resp.Body).Decode(&ack)
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("Ack: %#v", ack)
	}()
	select {}

	// if err := req.Write(conn); err != nil {
	// 	return nil, err
	// }
	w := newInsertStreamWriter(ctx, pw, rdr)
	// c.insertsStreamWriters = append(c.insertsStreamWriters, w)
	return w, nil
}

type TerminateClusterPayload struct {
	// DeleteTopicList is an optional list of Kafka topics to delete
	DeleteTopicList []string `json:"deleteTopicList,omitempty"`
}

func (c Client) TerminateCluster(ctx context.Context, payload TerminateClusterPayload) error {
	b := &bytes.Buffer{}
	if err := json.NewEncoder(b).Encode(&payload); err != nil {
		return err
	}
	req, err := c.makeRequest(ctx, TerminateClusterPath, http.MethodPost, b)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

type InfoResult map[string]interface{}

func (c Client) Info(ctx context.Context) (InfoResult, error) {
	result := InfoResult{}
	req, err := c.makeRequest(ctx, InfoPath, http.MethodGet, nil)
	if err != nil {
		return result, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return result, err
	}
	return result, nil
}

type HealthCheckResult struct {
	IsHealthy bool `json:"isHealthy"`
	Details   struct {
		Metastore struct {
			IsHealthy bool `json:"isHealthy"`
		} `json:"metastore"`
		Kafka struct {
			IsHealthy bool `json:"isHealthy"`
		} `json:"kafka"`
	} `json:"details"`
}

func (c Client) Healthcheck(ctx context.Context) (HealthCheckResult, error) {
	result := HealthCheckResult{}
	req, err := c.makeRequest(ctx, InfoPath, http.MethodGet, nil)
	if err != nil {
		return result, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return result, err
	}
	return result, nil
}

// Close gracefully closes all open connections in order to reuse TCP connections via keep-alive
func (c Client) Close() error {
	for _, rows := range c.rows {
		if rows == nil {
			continue
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	for _, wtr := range c.insertsStreamWriters {
		if wtr == nil {
			continue
		}
		if err := wtr.Close(); err != nil {
			return err
		}
	}
	return nil
}

// New constructs a new ksqlDB client
func New(connString string) *Client {
	client := &Client{
		baseURL: connString,
		client:  http.DefaultClient,
		http2: &http.Client{
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLS: func(network string, addr string, cfg *tls.Config) (net.Conn, error) {
					return net.Dial(network, addr)
				},
			},
		},
	}
	return client
}
