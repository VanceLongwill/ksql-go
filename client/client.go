package client

import (
	"crypto/tls"

	"net"
	"net/http"

	"golang.org/x/net/http2"
)

// Client is a ksqlDB client
type Client struct {
	http                 *http.Client
	baseURL              string
	rows                 []*QueryStreamRows
	insertsStreamWriters []*InsertsStreamWriter
}

// New constructs a new ksqlDB client
func New(baseURL string, options ...Option) *Client {
	client := &Client{
		baseURL: baseURL,
		http: &http.Client{
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLS: func(network string, addr string, cfg *tls.Config) (net.Conn, error) {
					return net.Dial(network, addr)
				},
			},
		},
	}
	for _, opt := range options {
		opt(client)
	}
	return client
}
