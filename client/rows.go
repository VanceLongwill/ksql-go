package ksql

import (
	"context"
	"encoding/json"
	"errors"
	"io"
)

var (
	// ErrColumnNumberMismatch is returned when the number of columns present doesn't match the destination slice length
	ErrColumnNumberMismatch = errors.New("unexpected number of columns")
	// ErrRowsClosed is returned when trying to interate over rows after the iterator has been closed
	ErrRowsClosed = errors.New("rows closed")
)

// Rows is almost the same as the standard library's driver.Rows interface except the Value type alias
type Rows interface {
	Columns() []string
	Close() error
	Next(dest []interface{}) error
}

// QueryRows is a row iterator for static queries
type QueryRows struct {
	res    []map[string]interface{}
	i      int
	closed bool
	columns
}

// Next implements the sql driver row interface used for interating over rows
func (q *QueryRows) Next(dest []interface{}) error {
	if q.closed {
		return ErrRowsClosed
	}
	if q.i > len(q.res)-1 {
		return io.EOF
	}
	row, exists := q.res[q.i]["row"]
	if !exists {
		return errors.New("unable to get row object")
	}
	rowMap, ok := row.(map[string]interface{})
	if !ok {
		return errors.New("row object has incorrect type")
	}
	cols, ok := rowMap["columns"]
	if !ok {
		return errors.New("unable to get columns from row object")
	}
	dest, ok = cols.([]interface{})
	if !ok {
		return errors.New("unable to convert columns to slice")
	}
	if err := q.columns.Validate(dest); err != nil {
		return err
	}
	q.i++
	return nil
}

// Close closes the rows interator
func (q *QueryRows) Close() error {
	q.closed = true
	return nil
}

// QueryStreamRows implements the standard libs Rows interface for reading DB rows
type QueryStreamRows struct {
	closed bool
	ctx    context.Context
	body   io.Closer
	dec    *json.Decoder
	columns
}

func (r *QueryStreamRows) next(dest []interface{}) error {
	if err := r.dec.Decode(&dest); err != nil {
		if r.closed {
			return ErrRowsClosed
		}
		return err
	}
	return r.columns.Validate(dest)
}

// Next reads another Row from the stream
func (r *QueryStreamRows) Next(dest []interface{}) error {
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
func (r *QueryStreamRows) Close() error {
	if r.body != nil {
		if err := r.body.Close(); err != nil {
			return err
		}
	}
	r.closed = true
	return nil
}
