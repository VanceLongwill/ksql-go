package stdlib

import (
	"context"
	"database/sql/driver"
	"errors"
	"strconv"

	"github.com/vancelongwill/ksql"
)

var (
	ErrTransactionsNotSupported = errors.New("Transactions are not supported by the kafka-go driver, please use Query or Exec instead")
)

type Conn struct {
	client             *ksql.Client
	preparedStatements map[string]PreparedStatement
	stmtNameCounter    int
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *Conn) Close() error {
	return c.client.Close()
}

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, ErrTransactionsNotSupported
}

func (c *Conn) Connect(context.Context) (driver.Conn, error) {
	return c, nil
}

func (c *Conn) Driver() driver.Driver {
	return &Driver{}
}

func (c *Conn) Ping(ctx context.Context) error {
	_, err := c.client.Info(ctx)
	return err
}

func (c *Conn) ResetSession(ctx context.Context) error {
	return nil
}

func (c *Conn) IsValid() bool {
	return true
}

type ExecResult struct{}

func (e *ExecResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (e *ExecResult) RowsAffected() (int64, error) {
	return 0, nil
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	_, err := c.client.Exec(ctx, ksql.ExecPayload{
		KSQL:              query,
		StreamsProperties: loadStreamsProperties(args),
	})
	if err != nil {
		return nil, err
	}
	return &ExecResult{}, nil
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

var (
	ErrInvalidQueryStrategy = errors.New("unrecognised query strategy")
)

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	conf, args := parseQueryArgs(args)
	q, err := buildStatement(query, args)
	if err != nil {
		return nil, err
	}
	switch conf.Strategy {
	case ksql.StreamQuery:
		return c.client.QueryStream(ctx, ksql.QueryStreamPayload{
			KSQL:       q,
			Properties: conf.StreamsProperties,
		})
	case ksql.StaticQuery:
		return c.client.Query(ctx, ksql.QueryPayload{
			KSQL:              q,
			StreamsProperties: conf.StreamsProperties,
		})

	}
	return nil, ErrInvalidQueryStrategy
}

func (c *Conn) Client() *ksql.Client {
	return c.client
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.stmtNameCounter++
	return PreparedStatement{
		Name: strconv.Itoa(c.stmtNameCounter),
		SQL:  query,
		conn: c,
	}, nil
}

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
