package ksql

import "database/sql/driver"

type columns struct {
	count int
	names []string
}

func (c columns) Validate(dest []driver.Value) error {
	if c.count == -1 {
		return nil
	}
	if len(dest) != c.count {
		return ErrColumnNumberMismatch
	}
	return nil
}

func (c columns) Columns() []string {
	if len(c.names) > 0 {
		return c.names
	}
	if c.count > 0 {
		// if there's no column names provided, just return empty strings
		cols := make([]string, c.count)
		for i := range cols {
			cols[i] = ""
		}
		return cols
	}
	return []string{}
}
