package ksql

import (
	"context"
	"database/sql/driver"

	"github.com/vancelongwill/ksql/client"
)

type Conn struct {
	client             *client.Client
	preparedStatements map[string]PreparedStatement
	stmtNameCounter    int
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *Conn) Close() error {
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, nil
}

func (c *Conn) Connect(context.Context) (driver.Conn, error) {
	return c, nil
}

func (c *Conn) Driver() driver.Driver {
	return &Driver{}
}

func (c *Conn) Ping(ctx context.Context) error {
	return nil
}

func (c *Conn) ResetSession(ctx context.Context) error {
	return nil
}

func (c *Conn) IsValid() bool {
	return true
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return nil, nil
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// check if the last arg is StreamsProperties
	last := args[len(args)-1]
	props := last.Value.(client.StreamsProperties)
	q, err := buildStatement(query, args)
	if err != nil {
		return nil, err
	}
	return c.client.Query(ctx, client.QueryPayload{
		KSQL:              q,
		StreamsProperties: props,
	})
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.stmtNameCounter++
	return PreparedStatement{
		Name: string(c.stmtNameCounter),
		SQL:  query,
		conn: c,
	}, nil
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, nil
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
)
