package client

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"golang.org/x/sync/errgroup"
)

// InsertsStreamTargetPayload represents the request body for initiating an inserts stream
type InsertsStreamTargetPayload struct {
	Target string `json:"target"`
}

// InsertsStreamAck represents an insert acknowledgement message in an inserts stream
type InsertsStreamAck struct {
	Status string `json:"status"`
	Seq    int64  `json:"seq"`
}

// InsertsStreamCloser gracefully terminates the stream
type InsertsStreamCloser struct {
	req  io.ReadCloser
	resp io.ReadCloser
}

// Close closes the request body thus terminating the stream
func (i *InsertsStreamCloser) Close() error {
	if err := i.req.Close(); err != nil {
		return err
	}
	if _, err := io.Copy(ioutil.Discard, i.resp); err != nil {
		return err
	}
	if err := i.resp.Close(); err != nil {
		return err
	}
	return nil
}

// InsertsStream allows you to insert rows into an existing ksqlDB stream. The stream must have already been created in ksqlDB.
func (c *ksqldb) InsertsStream(ctx context.Context, payload InsertsStreamTargetPayload) (*InsertsStreamWriter, error) {
	pr, pw := io.Pipe()
	req, err := makeRequest(ctx, c.baseURL, insertsStreamPath, http.MethodPost, ioutil.NopCloser(pr))
	if err != nil {
		return nil, err
	}
	ackCh := make(chan InsertsStreamAck)
	ackMap := make(map[int64]string)
	errCh := make(chan error, 1)
	enc := json.NewEncoder(pw)
	g, _ := errgroup.WithContext(context.Background())
	g.Go(func() error {
		return enc.Encode(&payload)
	})
	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	go func() {
		defer close(ackCh)
		sc := bufio.NewScanner(res.Body)
		for sc.Scan() {
			var ack InsertsStreamAck
			b := sc.Bytes()
			if err := json.Unmarshal(b, &ack); err != nil {
				errCh <- err
				close(errCh)
			}
			ackCh <- ack
		}
		if err := sc.Err(); err != nil {
			errCh <- err
			close(errCh)
		}
	}()
	i := &InsertsStreamWriter{
		enc:    enc,
		ackMap: ackMap,
		curr:   0,
		ackCh:  ackCh,
		errCh:  errCh,
		closer: &InsertsStreamCloser{req: pr, resp: res.Body},
	}
	c.insertsStreamWriters = append(c.insertsStreamWriters, i)
	return i, nil
}
