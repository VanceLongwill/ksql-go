package ksql

import (
	"context"
	"database/sql/driver"
)

type PreparedStatement struct {
	Name string
	SQL  string
	conn *Conn
}

func (p PreparedStatement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return p.conn.ExecContext(ctx, p.SQL, args)
}

func (p PreparedStatement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return p.conn.QueryContext(ctx, p.SQL, args)
}

func convertDriverValues(vals []driver.Value) []driver.NamedValue {
	nv := make([]driver.NamedValue, len(vals))
	for i, v := range vals {
		nv[i] = driver.NamedValue{Ordinal: i, Value: v}
	}
	return nv
}

func (p PreparedStatement) Exec(args []driver.Value) (driver.Result, error) {
	return p.ExecContext(context.Background(), convertDriverValues(args))
}

func (p PreparedStatement) Query(args []driver.Value) (driver.Rows, error) {
	return p.QueryContext(context.Background(), convertDriverValues(args))
}

func (p PreparedStatement) Close() error {
	return p.conn.Close()
}

func (p PreparedStatement) NumInput() int {
	return -1 // allow any number of args
}

var (
	_ driver.Stmt             = PreparedStatement{}
	_ driver.StmtExecContext  = PreparedStatement{}
	_ driver.StmtQueryContext = PreparedStatement{}
)
