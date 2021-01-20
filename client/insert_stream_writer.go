package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ErrAckUnsucessful signifies that the document couldn't be written the the stream
var ErrAckUnsucessful = errors.New("an ack was received but the status was not 'ok'")

// InsertsStreamWriter represents an inserts stream
type InsertsStreamWriter struct {
	mu     sync.Mutex
	enc    *json.Encoder
	ackMap map[int64]string
	curr   int64
	ackCh  <-chan InsertsStreamAck
	errCh  <-chan error
	closer io.Closer
}

// WriteJSON encodes and writes p to the inserts stream, and waits for the corresponding Ack to be received
func (i *InsertsStreamWriter) WriteJSON(ctx context.Context, p interface{}) error {
	i.mu.Lock()
	curr := i.curr
	if err := i.enc.Encode(&p); err != nil {
		return err
	}
	i.curr++
	i.mu.Unlock()
	// @TODO: cleaner way to process acks
	for {
		// check if the ack has been received in another goroutine
		if a, ok := i.ackMap[curr]; ok {
			if a != "ok" {
				return ErrAckUnsucessful
			}
			delete(i.ackMap, curr)
			return nil
		}
		select {
		case ack, ok := <-i.ackCh:
			if ok {
				i.ackMap[ack.Seq] = ack.Status
			}
		case err := <-i.errCh:
			return err
		case <-ctx.Done():
			return fmt.Errorf("context was cancelled before ack received: %w", ctx.Err())
		}
	}
}

// Close terminates the request and therefore inserts stream
func (i *InsertsStreamWriter) Close() error {
	return i.closer.Close()
}
