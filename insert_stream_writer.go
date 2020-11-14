package ksql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type InsertsStreamWriter struct {
	enc    *json.Encoder
	ackMap map[int64]string
	curr   int64
	ackCh  <-chan InsertsStreamAck
	errCh  <-chan error
}

func (i *InsertsStreamWriter) WriteJSON(ctx context.Context, p interface{}) error {
	curr := i.curr
	defer func() {
		i.curr++
	}()
	err := i.enc.Encode(&p)
	if err != nil {
		return err
	}
	if a, ok := i.ackMap[curr]; ok {
		if a != "ok" {
			return errors.New("Unsuccessful ack received")
		}
		return nil
	}
	for {
		select {
		case ack := <-i.ackCh:
			i.ackMap[ack.Seq] = ack.Status
			if a, ok := i.ackMap[curr]; ok {
				if a != "ok" {
					return errors.New("Unsuccessful ack received")
				}
				return nil
			}
		case err := <-i.errCh:
			return err
		case <-ctx.Done():
			return fmt.Errorf("context was cancelled before ack received: %w", ctx.Err())
		}
	}
}
