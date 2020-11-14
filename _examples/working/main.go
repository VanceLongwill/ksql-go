package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/sync/errgroup"
)

type DataRow struct {
	K  string `json:"k"`
	V1 int    `json:"v1"`
	V2 string `json:"v2"`
	V3 bool   `json:"v3"`
}

type InsertsStreamAck struct {
	Status string `json:"status"`
	Seq    int64  `json:"seq"`
}

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
		log.Printf("FOUND ACK %d %s", curr, a)
		if a != "ok" {
			return errors.New("Unsuccessful ack received")
		}
		return nil
	}
	for {
		select {
		case ack := <-i.ackCh:
			log.Printf("got ack %#v", ack)
			i.ackMap[ack.Seq] = ack.Status
			if a, ok := i.ackMap[curr]; ok {
				log.Printf("FOUND ACK %d %s", curr, a)
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

func InsertsStream(ctx context.Context) (*InsertsStreamWriter, error) {
	pr, pw := io.Pipe()
	req, err := http.NewRequest("POST", "http://0.0.0.0:8088/inserts-stream", ioutil.NopCloser(pr))
	if err != nil {
		return nil, err
	}
	ackCh := make(chan InsertsStreamAck)
	ackMap := map[int64]string{}
	enc := json.NewEncoder(pw)
	errCh := make(chan error)
	resCh := make(chan *http.Response, 1)
	g, _ := errgroup.WithContext(context.Background())
	g.Go(func() error {
		return enc.Encode(map[string]string{"target": "s1"})
	})
	g.Go(func() error {
		t := &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(network string, addr string, cfg *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		}
		c := http.Client{Transport: t}
		res, err := c.Do(req)
		if err != nil {
			return err
		}
		log.Printf("Got: %#v", res)
		resCh <- res
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	res := <-resCh
	go func() {
		defer close(ackCh)
		sc := bufio.NewScanner(res.Body)
		for sc.Scan() {
			var ack InsertsStreamAck
			b := sc.Bytes()
			if err := json.Unmarshal(b, &ack); err != nil {
				errCh <- err
			}
			log.Printf("BBB: %#v", ack)
			ackCh <- ack
		}
		if err := sc.Err(); err != nil {
			errCh <- err
		}
	}()

	return &InsertsStreamWriter{
		enc:    enc,
		ackMap: ackMap,
		curr:   0,
		ackCh:  ackCh,
		errCh:  errCh,
	}, nil

}

func main() {
	wtr, err := InsertsStream(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	dataRows := []DataRow{
		{K: "something", V1: 99, V2: "yes", V3: true},
		{K: "somethingelse", V1: 19292, V2: "asdasd", V3: false},
		{K: "somethingelse", V1: 19292, V2: "asdasd", V3: false},
	}
	for _, row := range dataRows {
		time.Sleep(1 * time.Second)
		err := wtr.WriteJSON(context.Background(), &row)
		if err != nil {
			log.Fatalln("Unable to write JSON")
		}
	}
}
