package stdlib

import (
	"context"
	"database/sql/driver"
	"errors"
	"strconv"

	ksql "github.com/vancelongwill/ksql/client"
)

var (
	// ErrTransactionsNotSupported is a placeholder returned by sql driver transaction related methods, given that no such functionality exists currently in ksqlDB
	ErrTransactionsNotSupported = errors.New("Transactions are not supported by the kafka-go driver, please use Query or Exec instead")

	// ErrInvalidQueryStrategy is returned when the provided query strategy is not supported or doesn't exist
	ErrInvalidQueryStrategy = errors.New("unrecognised query strategy")
)

//go:generate mockgen -source ../client/client.go -destination mocks/client.go -package mocks

// conn provides the driver.conn interface for interacting with the ksqlDB client
type conn struct {
	client             ksql.Client
	preparedStatements map[string]PreparedStatement
	stmtNameCounter    int
}

// Prepare a SQL query. Note that there are no optimizations here and this method is only provided for compatibility reasons.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// Close the underlying client connection
func (c *conn) Close() error {
	return c.client.Close()
}

// Begin is not supported but implemented here for compatibility
func (c *conn) Begin() (driver.Tx, error) {
	panic(ErrTransactionsNotSupported)
}

// Connect simply returns the connection
func (c *conn) Connect(context.Context) (driver.Conn, error) {
	return c, nil
}

// Driver returns a Driver singleton
func (c *conn) Driver() driver.Driver {
	return &Driver{}
}

// Ping checks the status of the ksqlDB cluster using the /info endpoint
func (c *conn) Ping(ctx context.Context) error {
	_, err := c.client.Info(ctx)
	return err
}

// ResetSession is a placeholder for compatibility
func (c *conn) ResetSession(ctx context.Context) error {
	return nil
}

// IsValid is a placeholder for compatibility
func (c *conn) IsValid() bool {
	return true
}

type execResult struct{}

// LastInsertId is a placeholder for compatibility
func (e *execResult) LastInsertId() (int64, error) {
	return 0, nil
}

// RowsAffected is a placeholder for compatibility
func (e *execResult) RowsAffected() (int64, error) {
	return 0, nil
}

// ExecContext executes any arbitrary ksql statements, except queries
func (c *conn) ExecContext(ctx context.Context, stmt string, args []driver.NamedValue) (driver.Result, error) {
	sql, err := buildStatement(stmt, args)
	if err != nil {
		return nil, err
	}
	_, err = c.client.Exec(ctx, ksql.ExecPayload{
		KSQL:              sql,
		StreamsProperties: loadStreamsProperties(args),
	})
	if err != nil {
		return nil, err
	}
	return &execResult{}, nil
}

func loadStreamsProperties(args []driver.NamedValue) ksql.StreamsProperties {
	var props ksql.StreamsProperties
	// check if the last arg is StreamsProperties
	if len(args) > 0 {
		last := args[len(args)-1]
		if p, ok := last.Value.(ksql.StreamsProperties); ok {
			props = p
		}
	}
	return props
}

func parseQueryArgs(args []driver.NamedValue) (*ksql.QueryConfig, []driver.NamedValue) {
	var (
		config       = ksql.DefaultQueryConfig
		filteredArgs []driver.NamedValue
	)
	for _, arg := range args {
		switch v := arg.Value.(type) {
		case *ksql.QueryConfig:
			config = v
		default:
			filteredArgs = append(filteredArgs, arg)
		}
	}

	return config, filteredArgs
}

// QueryContext runs a SELECT query via the ksqlDB client
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	conf, args := parseQueryArgs(args)
	q, err := buildStatement(query, args)
	if err != nil {
		return nil, err
	}
	switch conf.Strategy {
	case ksql.StreamQuery:
		rows, err := c.client.QueryStream(ctx, ksql.QueryStreamPayload{
			KSQL:       q,
			Properties: conf.StreamsProperties,
		})
		return &rowWrapper{rows}, err
	case ksql.StaticQuery:
		rows, err := c.client.Query(ctx, ksql.QueryPayload{
			KSQL:              q,
			StreamsProperties: conf.StreamsProperties,
		})
		return &rowWrapper{rows}, err

	}
	return nil, ErrInvalidQueryStrategy
}

// Client exposes the underlying ksql client instance
func (c *conn) Client() ksql.Client {
	return c.client
}

// PrepareContext is a placeholder, prepared statements are not supported in ksqlDB
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.stmtNameCounter++
	return PreparedStatement{
		Name: strconv.Itoa(c.stmtNameCounter),
		SQL:  query,
		conn: c,
	}, nil
}

// BeginTx is a placeholder, transactions are not supported
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	panic(ErrTransactionsNotSupported)
}

// CheckNamedValue is used here to filter out config options from the final args
func (c *conn) CheckNamedValue(val *driver.NamedValue) error {
	if _, ok := val.Value.(ksql.QueryConfig); ok {
		return driver.ErrRemoveArgument
	}
	return nil
}

func newConn(client ksql.Client) *conn {
	return &conn{
		client:             client,
		preparedStatements: map[string]PreparedStatement{},
	}
}

// Check that Conn implements the required interfaces
var (
	_ driver.Conn = &conn{}

	_ driver.Connector       = &conn{}
	_ driver.Pinger          = &conn{}
	_ driver.SessionResetter = &conn{}
	_ driver.Validator       = &conn{}

	_ driver.ExecerContext      = &conn{}
	_ driver.QueryerContext     = &conn{}
	_ driver.ConnPrepareContext = &conn{}
	_ driver.ConnBeginTx        = &conn{}

	_ driver.NamedValueChecker = &conn{}
)
