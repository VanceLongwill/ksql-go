package client

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
)

var (
	ErrColumnNumberMismatch = errors.New("unexpected number of columns")
)

// Rows implements the standard libs Rows interface for reading DB rows
type Rows struct {
	body     io.Closer
	dec      *json.Decoder
	colCount int
	colNames []string
}

// Columns is a placeholder for returning the names of the columns.
func (r *Rows) Columns() []string {
	if len(r.colNames) > 0 {
		return r.colNames
	}
	// if there's no column names provided, just return empty strings
	cols := make([]string, r.colCount)
	for i := range cols {
		cols[i] = ""
	}
	return cols
}

// Next reads another Row from the stream
func (r *Rows) Next(dest []driver.Value) error {
	var m QueryResult
	if err := r.dec.Decode(&m); err != nil {
		return err
	}
	copy(m.Row.Columns, dest)
	if len(dest) != len(m.Row.Columns) {
		return ErrColumnNumberMismatch
	}
	return nil
}

// Close safely closes the response, allowing connections to be kept alive
func (r *Rows) Close() error {
	return r.body.Close()
}

// NewRows creates a Rows reader
func NewRows(rdr io.ReadCloser) *Rows {
	return &Rows{
		body: Emptier{rdr},
		dec:  json.NewDecoder(rdr),
	}
}
