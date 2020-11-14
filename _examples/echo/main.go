package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"

	"golang.org/x/net/http2"
	"golang.org/x/sync/errgroup"
)

type DataRow struct {
	K  string `json:"k"`
	V1 int    `json:"v1"`
	V2 string `json:"v2"`
	V3 bool   `json:"v3"`
}

type InsertsStreamWriter struct {
	targetStream string
	req          io.Writer
	resp         io.Reader
}

func (i *InsertsStreamWriter) ReadAck() error {
	sc := bufio.NewScanner(i.resp)
	for sc.Scan() {
		fmt.Println(sc.Bytes())
	}
	return sc.Err()
}

func (i *InsertsStreamWriter) WriteJSON(ctx context.Context, p interface{}) error {
	return json.NewEncoder(i.req).Encode(p)
}

func (i *InsertsStreamWriter) Init() error {
	respRw := &bytes.Buffer{}
	pr, pw := io.Pipe()
	i.req = pw
	req, err := http.NewRequest("POST", "http://0.0.0.0:8088/inserts-stream", ioutil.NopCloser(pr))
	if err != nil {
		return err
	}
	g, _ := errgroup.WithContext(context.Background())
	g.Go(func() error {
		return json.NewEncoder(pw).Encode(map[string]string{"target": i.targetStream})
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
		n, err := io.Copy(respRw, res.Body)
		log.Fatalf("copied %d, %v", n, err)
		return nil
	})
	i.resp = respRw
	return g.Wait()
}

func NewInsertsStreamWriter() *InsertsStreamWriter {
	i := &InsertsStreamWriter{targetStream: "s1"}
	return i
}

func main() {
	wtr := NewInsertsStreamWriter()
	done := make(chan struct{})

	go func() {
		if err := wtr.Init(); err != nil {
			log.Fatalln(err)
		}
		done <- struct{}{}
	}()
	dataRows := []DataRow{
		{K: "something", V1: 99, V2: "yes", V3: true},
		{K: "somethingelse", V1: 19292, V2: "asdasd", V3: false},
		{K: "somethingelse", V1: 19292, V2: "asdasd", V3: false},
	}
	for _, row := range dataRows {
		fmt.Printf("Doing row %#v \n", row)
		if err := wtr.WriteJSON(context.Background(), &row); err != nil {
			log.Fatalln(err)
		}
		if err := wtr.ReadAck(); err != nil {
			log.Fatalln(err)
		}
	}
	<-done
}
