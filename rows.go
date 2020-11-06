package ksql

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
)

var (
	ErrColumnNumberMismatch = errors.New("unexpected number of columns")
	ErrRowsClosed           = errors.New("rows closed")
)

// Rows implements the standard libs Rows interface for reading DB rows
type Rows struct {
	closed   bool
	ctx      context.Context
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

func (r *Rows) next(dest []driver.Value) error {
	if err := r.dec.Decode(&dest); err != nil {
		if r.closed {
			return ErrRowsClosed
		}
		return err
	}
	if len(dest) != r.colCount {
		return ErrColumnNumberMismatch
	}
	return nil
}

// Next reads another Row from the stream
func (r *Rows) Next(dest []driver.Value) error {
	errChan := make(chan error)
	go func() {
		errChan <- r.next(dest)
	}()
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	case err := <-errChan:
		return err
	}
}

// Close safely closes the response, allowing connections to be kept alive
func (r *Rows) Close() error {
	if r.body != nil {
		if err := r.body.Close(); err != nil {
			return err
		}
	}
	r.closed = true
	return nil
}
