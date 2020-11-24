package stdlib

import (
	"context"
	"database/sql/driver"
	"errors"
	"strconv"

	"github.com/vancelongwill/ksql"
)

var (
	// ErrTransactionsNotSupported is a placeholder returned by sql driver transaction related methods, given that no such functionality exists currently in ksqlDB
	ErrTransactionsNotSupported = errors.New("Transactions are not supported by the kafka-go driver, please use Query or Exec instead")

	// ErrInvalidQueryStrategy is returned when the provided query strategy is not supported or doesn't exist
	ErrInvalidQueryStrategy = errors.New("unrecognised query strategy")
)

// Conn provides the driver.Conn interface for interacting with the ksqlDB client
type Conn struct {
	client             *ksql.Client
	preparedStatements map[string]PreparedStatement
	stmtNameCounter    int
}

// Prepare a SQL query. Note that there are no optimizations here and this method is only provided for compatibility reasons.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// Close the underlying client connection
func (c *Conn) Close() error {
	return c.client.Close()
}

// Begin is not supported but implemented here for compatibility
func (c *Conn) Begin() (driver.Tx, error) {
	return nil, ErrTransactionsNotSupported
}

// Connect simply returns the connection
func (c *Conn) Connect(context.Context) (driver.Conn, error) {
	return c, nil
}

// Driver returns a Driver singleton
func (c *Conn) Driver() driver.Driver {
	return &Driver{}
}

// Ping checks the status of the ksqlDB cluster using the /info endpoint
func (c *Conn) Ping(ctx context.Context) error {
	_, err := c.client.Info(ctx)
	return err
}

// ResetSession is a placeholder for compatibility
func (c *Conn) ResetSession(ctx context.Context) error {
	return nil
}

// IsValid is a placeholder for compatibility
func (c *Conn) IsValid() bool {
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
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	_, err := c.client.Exec(ctx, ksql.ExecPayload{
		KSQL:              query,
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
		props = last.Value.(ksql.StreamsProperties)
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

type rowWrapper struct {
	rows ksql.Rows
}

func (q *rowWrapper) Columns() []string {
	return q.rows.Columns()
}

func (q *rowWrapper) Close() error {
	return q.rows.Close()
}

func (q *rowWrapper) Next(dest []driver.Value) error {
	in := make([]interface{}, len(dest))
	if err := q.rows.Next(in); err != nil {
		return err
	}
	for i := range dest {
		dest[i] = in[i].(driver.Value)
	}
	return nil
}

// QueryContext runs a SELECT query via the ksqlDB client
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
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
func (c *Conn) Client() *ksql.Client {
	return c.client
}

// PrepareContext is a placeholder, prepared statements are not supported in ksqlDB
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.stmtNameCounter++
	return PreparedStatement{
		Name: strconv.Itoa(c.stmtNameCounter),
		SQL:  query,
		conn: c,
	}, nil
}

// BeginTx is a placeholder, transactions are not supported
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, ErrTransactionsNotSupported
}

// CheckNamedValue is used here to filter out config options from the final args
func (c *Conn) CheckNamedValue(val *driver.NamedValue) error {
	if _, ok := val.Value.(ksql.QueryConfig); ok {
		return driver.ErrRemoveArgument
	}
	return nil
}

// Check that Conn implements the required interfaces
var (
	_ driver.Conn = &Conn{}

	_ driver.Connector       = &Conn{}
	_ driver.Pinger          = &Conn{}
	_ driver.SessionResetter = &Conn{}
	_ driver.Validator       = &Conn{}

	_ driver.ExecerContext      = &Conn{}
	_ driver.QueryerContext     = &Conn{}
	_ driver.ConnPrepareContext = &Conn{}
	_ driver.ConnBeginTx        = &Conn{}

	_ driver.NamedValueChecker = &Conn{}
)
