package main

import (
	"crypto/tls"
	"encoding/json"
	"golang.org/x/net/http2"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

type DataRow struct {
	K  string `json:"k"`
	V1 int    `json:"v1"`
	V2 string `json:"v2"`
	V3 bool   `json:"v3"`
}

type InsertsStreamWriter struct {
}

func (i *InsertsStreamWriter) Init() {

	go func() {
		err := json.NewEncoder(pw).Encode(map[string]string{"target": "s1"})
		if err != nil {
			errCh <- err
		}
}

func NewInsertsStreamWriter() *InsertsStreamWriter {
	pr, pw := io.Pipe()
	req, err := http.NewRequest("POST", "http://0.0.0.0:8088/inserts-stream", ioutil.NopCloser(pr))
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		err := json.NewEncoder(pw).Encode(map[string]string{"target": "s1"})
		if err != nil {
			errCh <- err
		}

		dataRows := []DataRow{
			{K: "something", V1: 99, V2: "yes", V3: true},
			{K: "somethingelse", V1: 19292, V2: "asdasd", V3: false},
			{K: "somethingelse", V1: 19292, V2: "asdasd", V3: false},
		}
		for _, row := range dataRows {
			time.Sleep(1 * time.Second)
			err := json.NewEncoder(pw).Encode(&row)
			if err != nil {
				log.Fatal(err)
			}
			// fmt.Fprintf(pw, "It is now %v\n", time.Now())
		}
	}()
	go func() {
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
		n, err := io.Copy(os.Stdout, res.Body)
		log.Fatalf("copied %d, %v", n, err)
	}()

}

func main() {
	pr, pw := io.Pipe()
	req, err := http.NewRequest("POST", "http://0.0.0.0:8088/inserts-stream", ioutil.NopCloser(pr))
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		err := json.NewEncoder(pw).Encode(map[string]string{"target": "s1"})
		if err != nil {
			log.Fatal(err)
		}

		dataRows := []DataRow{
			{K: "something", V1: 99, V2: "yes", V3: true},
			{K: "somethingelse", V1: 19292, V2: "asdasd", V3: false},
			{K: "somethingelse", V1: 19292, V2: "asdasd", V3: false},
		}
		for _, row := range dataRows {
			time.Sleep(1 * time.Second)
			err := json.NewEncoder(pw).Encode(&row)
			if err != nil {
				log.Fatal(err)
			}
			// fmt.Fprintf(pw, "It is now %v\n", time.Now())
		}
	}()
	go func() {
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
		n, err := io.Copy(os.Stdout, res.Body)
		log.Fatalf("copied %d, %v", n, err)
	}()
	select {}
}
