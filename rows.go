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
	closed bool
	ctx    context.Context
	body   io.Closer
	dec    *json.Decoder
	columns
}

func (r *Rows) next(dest []driver.Value) error {
	if err := r.dec.Decode(&dest); err != nil {
		if r.closed {
			return ErrRowsClosed
		}
		return err
	}
	return r.columns.Validate(dest)
}

// Next reads another Row from the stream
func (r *Rows) Next(dest []driver.Value) error {
	errChan := make(chan error)
	go func() {
		errChan <- r.next(dest)
		close(errChan)
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
