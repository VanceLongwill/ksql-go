package client

import (
	"net/http"
)

// Option represents a function option for the ksqlDB client
type Option func(*ksqldb)

// WithHTTPClient is an option for the ksqlDB client which allows the user to override the default http client
func WithHTTPClient(client *http.Client) Option {
	return func(c *ksqldb) {
		c.http = client
	}
}
