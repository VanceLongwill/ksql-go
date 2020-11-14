package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
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

type Inserter struct {
	enc    *json.Encoder
	ackMap map[int64]string
	curr   int64
	ackCh  <-chan InsertsStreamAck
}

func (i *Inserter) WriteJSON(ctx context.Context, p interface{}) error {
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
	for ack := range i.ackCh {
		log.Printf("got ack %#v", ack)
		i.ackMap[ack.Seq] = ack.Status
		if a, ok := i.ackMap[curr]; ok {
			log.Printf("FOUND ACK %d %s", curr, a)
			if a != "ok" {
				return errors.New("Unsuccessful ack received")
			}
			return nil
		}
	}
	return errors.New("Ack channel was closed before ack found")
}

func main() {
	pr, pw := io.Pipe()
	req, err := http.NewRequest("POST", "http://0.0.0.0:8088/inserts-stream", ioutil.NopCloser(pr))
	if err != nil {
		log.Fatal(err)
	}
	ackCh := make(chan InsertsStreamAck)
	ackMap := map[int64]string{}
	enc := json.NewEncoder(pw)
	go func() {
		err := enc.Encode(map[string]string{"target": "s1"})
		if err != nil {
			log.Fatal(err)
		}
		// dataRows := []DataRow{
		// 	{K: "something", V1: 99, V2: "yes", V3: true},
		// 	{K: "somethingelse", V1: 19292, V2: "asdasd", V3: false},
		// 	{K: "somethingelse", V1: 19292, V2: "asdasd", V3: false},
		// }
		// for i, row := range dataRows {
		// 	time.Sleep(1 * time.Second)
		// 	err := json.NewEncoder(pw).Encode(&row)
		// 	if err != nil {
		// 		log.Fatal(err)
		// 	}
		// 	if a, ok := ackMap[int64(i)]; ok {
		// 		log.Printf("FOUND ACK %d %s", i, a)
		// 		continue
		// 	}
		// 	for ack := range ackCh {
		// 		log.Printf("got ack %#v", ack)
		// 		ackMap[ack.Seq] = ack.Status
		// 		if a, ok := ackMap[int64(i)]; ok {
		// 			log.Printf("FOUND ACK %d %s", i, a)
		// 			break
		// 		}
		// 	}
		// }
	}()
	go func() {
		defer close(ackCh)
		t := &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(network string, addr string, cfg *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		}
		c := http.Client{Transport: t}
		res, err := c.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Got: %#v", res)
		sc := bufio.NewScanner(res.Body)
		for sc.Scan() {
			var ack InsertsStreamAck
			b := sc.Bytes()
			if err := json.Unmarshal(b, &ack); err != nil {
				log.Fatalln(err)
			}
			log.Printf("BBB: %#v", ack)
			ackCh <- ack
		}
		if err := sc.Err(); err != nil {
			log.Fatalln(err)
		}
	}()

	go func() {
		wtr := &Inserter{
			enc:    enc,
			ackMap: ackMap,
			curr:   0,
			ackCh:  ackCh,
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

	}()
	select {}
}
