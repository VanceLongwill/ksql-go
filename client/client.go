package client

import (
	"context"
	"crypto/tls"

	"net"
	"net/http"

	"golang.org/x/net/http2"
)

// ksqldb is a ksqlDB client
type ksqldb struct {
	http                 *http.Client
	baseURL              string
	rows                 []*QueryStreamRows
	insertsStreamWriters []*InsertsStreamWriter
}

// Client is a ksqlDB client
type Client interface {
	// Close closes all open connections
	Close() error
	// Describe returns information about an object
	Describe(ctx context.Context, source string) (DescribeResult, error)
	// Exec runs KSQL statements which can be anything except SELECT
	Exec(ctx context.Context, params ExecPayload) ([]ExecResult, error)
	// Explain returns details of the execution plan for a query or expression
	Explain(ctx context.Context, queryNameOrExpression string) (ExplainResult, error)
	// Healthcheck gets basic health information from the ksqlDB cluster
	Healthcheck(ctx context.Context) (HealthcheckResult, error)
	// Info returns status information about the ksqlDB cluster
	Info(ctx context.Context) (InfoResult, error)
	// InsertsStream allows you to insert rows into an existing ksqlDB stream. The stream must have already been created in ksqlDB.
	InsertsStream(ctx context.Context, payload InsertsStreamTargetPayload) (*InsertsStreamWriter, error)
	// ListQueries is a convenience method which executes a `LIST QUERIES;` operation
	ListQueries(ctx context.Context) (ListQueriesResult, error)
	// ListTables is a convenience method which executes a `LIST TABLES;` operation
	ListTables(ctx context.Context) (ListTablesResult, error)
	// ListStreams is a convenience method which executes a `LIST STREAMS;` operation
	ListStreams(ctx context.Context) (ListStreamsResult, error)
	// ListProperties is a convenience method which executes a `LIST PROPERTIES;` operation
	ListProperties(ctx context.Context) (ListPropertiesResult, error)
	// Query runs a KSQL query and returns a cursor. For streaming results use the QueryStream method.
	Query(ctx context.Context, payload QueryPayload) (*QueryRows, error)
	// QueryStream runs a streaming push & pull query
	QueryStream(ctx context.Context, payload QueryStreamPayload) (*QueryStreamRows, error)
	// TerminateCluster terminates a running ksqlDB cluster
	TerminateCluster(ctx context.Context, payload TerminateClusterPayload) error
}

// New constructs a new ksqlDB client
func New(baseURL string, options ...Option) Client {
	client := &ksqldb{
		baseURL: baseURL,
		http: &http.Client{
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLS: func(network string, addr string, cfg *tls.Config) (net.Conn, error) {
					return net.Dial(network, addr)
				},
			},
		},
	}
	for _, opt := range options {
		opt(client)
	}
	return client
}
