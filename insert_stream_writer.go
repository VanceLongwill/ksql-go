package ksql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

type InsertsStreamWriter struct {
	mu      sync.Mutex
	ctx     context.Context
	conn    net.Conn
	req     io.Writer
	resp    io.Reader
	acks    map[int64]string
	enc     *json.Encoder
	err     error
	curr    int64
	timeout time.Duration
}

// Write a JSON object representing the values to insert
func (i *InsertsStreamWriter) WriteJSON(p interface{}) error {
	i.mu.Lock()
	defer func() {
		i.curr++ // increment the sequence number
		i.mu.Unlock()
	}()
	if i.err != nil {
		return i.err
	}
	if err := i.enc.Encode(p); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), i.timeout)
	defer cancel()
	if err := i.readAcksUntil(ctx); err != nil {
		if err == context.DeadlineExceeded {
			return fmt.Errorf("timed out while waiting for ACK: %w", err)
		}
		return err
	}
	return nil
}

func (i *InsertsStreamWriter) readAcksUntil(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()
	for {
		select {
		case <-i.ctx.Done():
			return ctx.Err()
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
	// b := bufio.NewReader(i.conn)
	// s, err := b.ReadString('\n')
	// if err != nil {
	// 	if err == io.EOF {
	// 		return nil
	// 	}
	// 	return err
	// }
	// fmt.Println(s)
	if err := json.NewDecoder(i.resp).Decode(&ack); err != nil {
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("unable to decode ack %w", err)
	}
	i.acks[ack.Seq] = ack.Status
	return nil
}

func (i *InsertsStreamWriter) Close() error {
	// if err := i.conn.Close(); err != nil {
	// 	return err
	// }
	return nil
}

func newInsertStreamWriter(ctx context.Context, req io.Writer, resp io.Reader) *InsertsStreamWriter {
	i := &InsertsStreamWriter{}
	i.ctx = ctx
	i.mu = sync.Mutex{}
	i.acks = map[int64]string{}
	// i.conn = conn
	i.resp = io.TeeReader(resp, os.Stdout)
	i.enc = json.NewEncoder(req)
	i.timeout = 2 * time.Second
	return i
}
