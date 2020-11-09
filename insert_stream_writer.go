package ksql

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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
	s       *bufio.Scanner
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
	return nil
	// ctx, cancel := context.WithTimeout(context.Background(), i.timeout)
	// defer cancel()
	if status, ok := i.acks[i.curr]; ok {
		if status == "ok" {
			return nil
		} else {
			return ErrAckUnsucessful
		}
	}
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		for {
			if err := i.readAck(i.curr); err == nil {
				errCh <- err
				return
			}
		}
		// fmt.Printf("abc %#v\n", i.acks)
	}()

	timer := time.NewTimer(i.timeout)
	defer timer.Stop()
	select {
	case err := <-errCh:
		return err
	case <-timer.C:
		return errors.New("timeout exceeded")
	case <-i.ctx.Done():
		return i.ctx.Err()
	}

	// if err := i.readAcksUntil(ctx); err != nil {
	// 	if err == context.DeadlineExceeded {
	// 		return fmt.Errorf("timed out while waiting for ACK: %w", err)
	// 	}
	// 	return err
	// }
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
			// fmt.Println("tick")
			// fmt.Printf("%#v \n", i.acks)
			if status, ok := i.acks[i.curr]; ok {
				if status == "ok" {
					return nil
				} else {
					return ErrAckUnsucessful
				}
			}
			if err := i.readAck(8); err != nil {
				return err
			}
		}
	}
}

func (i *InsertsStreamWriter) readAck(n int64) error {
	var ack InsertsStreamAck
	sc := bufio.NewScanner(i.resp)
	if len(sc.Bytes()) > 0 {
		fmt.Println("FIESTY", sc.Bytes())
	}
	// fmt.Println("here1")
	for sc.Scan() {
		fmt.Println("here")
		if b := i.s.Bytes(); len(b) > 0 {
			fmt.Println("BBB", b)
			if err := json.Unmarshal(i.s.Bytes(), &ack); err != nil {
				return fmt.Errorf("unable to decode ack %w", err)
			}
			fmt.Printf("Found ack %#v\n", ack)
			i.acks[ack.Seq] = ack.Status
			if ack.Seq == n {
				return nil
			}
		}
	}
	if err := i.s.Err(); err != nil {
		return err
	}
	// if err := json.NewDecoder(i.resp).Decode(&ack); err != nil {
	// 	if errors.Is(err, io.EOF) {
	// 		return nil
	// 	}
	// 	return err
	// }
	return errors.New("Couldn't get ack")
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
	i.s = bufio.NewScanner(bufio.NewReader(i.resp))
	i.enc = json.NewEncoder(req)
	i.timeout = 15 * time.Second
	return i
}
