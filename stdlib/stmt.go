package stdlib

import (
	"context"
	"database/sql/driver"
)

// PreparedStatement imitates a pre loaded sql statement
type PreparedStatement struct {
	Name string
	SQL  string
	conn *Conn
}

// ExecContext is equivalent to running the given sql statement via the ExecerContext interface
func (p PreparedStatement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return p.conn.ExecContext(ctx, p.SQL, args)
}

// QueryContext is equivalent to running the given sql statement via the QueryerContext interface
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

// Exec is equivalent to running the given sql statement via the Execer interface
func (p PreparedStatement) Exec(args []driver.Value) (driver.Result, error) {
	return p.ExecContext(context.Background(), convertDriverValues(args))
}

// Query is equivalent to running the given sql statement via the Queryer interface
func (p PreparedStatement) Query(args []driver.Value) (driver.Rows, error) {
	return p.QueryContext(context.Background(), convertDriverValues(args))
}

// Close closes the ksqlDB connection
func (p PreparedStatement) Close() error {
	return p.conn.Close()
}

// NumInput always returns -1 to allow any number of arguments
func (p PreparedStatement) NumInput() int {
	return -1 // allow any number of args
}

var (
	_ driver.Stmt             = PreparedStatement{}
	_ driver.StmtExecContext  = PreparedStatement{}
	_ driver.StmtQueryContext = PreparedStatement{}
)
