package ksql

import (
	"net/http"
)

// Option represents a function option for the ksqlDB client
type Option func(*Client)

// WithHTTPClient is an option for the ksqlDB client which allows the user to override the default http client
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		c.http = client
	}
}
