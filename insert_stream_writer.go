package ksql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
)

type InsertsStreamWriter struct {
	mu      sync.Mutex
	req     io.Writer
	resp    io.ReadCloser
	acks    map[int64]string
	dec     *json.Decoder
	err     error
	curr    int64
	timeout time.Duration
}

var newLineBytes = []byte("\n")

// Write a JSON object representing the values to insert
func (i *InsertsStreamWriter) Write(p []byte) (int, error) {
	i.mu.Lock()
	defer func() {
		i.curr++ // increment the sequence number
		i.mu.Unlock()
	}()
	if i.err != nil {
		return 0, i.err
	}
	n, err := i.req.Write(p)
	if err != nil {
		return 0, err
	}
	nl, err := i.req.Write(newLineBytes)
	if err != nil {
		return n, err
	}
	n += nl

	ctx, cancel := context.WithTimeout(context.Background(), i.timeout)
	defer cancel()
	if err := i.readAcksUntil(ctx); err != nil {
		if err == context.DeadlineExceeded {
			return 0, fmt.Errorf("timed out while waiting for ACK: %w", err)
		}
		return 0, err
	}

	return n, nil
}

func (i *InsertsStreamWriter) readAcksUntil(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if status, ok := i.acks[i.curr]; ok {
				if status == "ok" {
					return nil
				} else {
					return ErrAckUnsucessful
				}
			}
			if err := i.readAck(); err != nil {
				return err
			}

		}
	}
}

func (i *InsertsStreamWriter) readAck() error {
	var ack InsertsStreamAck
	if err := i.dec.Decode(&ack); err != nil {
		return err
	}
	i.acks[ack.Seq] = ack.Status
	return nil
}

func (i *InsertsStreamWriter) Close() error {
	if err := i.resp.Close(); err != nil {
		return err
	}
	return nil
}

func newInsertStreamWriter(req io.Writer, resp io.ReadCloser) *InsertsStreamWriter {
	i := &InsertsStreamWriter{}
	i.mu = sync.Mutex{}
	i.acks = map[int64]string{}
	i.req = req
	i.resp = Emptier{resp}
	i.dec = json.NewDecoder(i.resp)
	i.timeout = 5 * time.Second
	return i
}
