package stdlib

import (
	"database/sql/driver"

	ksql "github.com/vancelongwill/ksql-go/client"
)

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
	j := 0
	for i := range dest {
		if v, ok := in[i].(driver.Value); ok {
			dest[j] = v
			j++
		}
	}
	return nil
}
