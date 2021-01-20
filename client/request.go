package client

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"path"
)

var (
	// acceptJSON is the http accept header for requesting JSON responses
	acceptJSON = "application/vnd.ksql.v1+json"
	// acceptDelim is the http accept header for requesting DELIMITED responses (default for push & pull queries)
	acceptDelim = "application/vnd.ksqlapi.delimited.v1"

	// ksqlDB endpoints

	queryPath            = "/query"
	execPath             = "/ksql"
	queryStreamPath      = "/query-stream"
	closeQueryPath       = "/close-query"
	insertsStreamPath    = "/inserts-stream"
	terminateClusterPath = "/ksql/terminate"
	infoPath             = "/info"
	healthCheckPath      = "/healthcheck"
)

func makeRequest(ctx context.Context, baseURL string, slug string, method string, rdr io.Reader) (*http.Request, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, slug)
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rdr)
	if err != nil {
		return nil, err
	}
	switch slug {
	case queryStreamPath, insertsStreamPath:
		req.Header.Add("Accept", acceptDelim)
		req.Header.Add("Content-Type", acceptDelim)
	default:
		req.Header.Add("Accept", acceptJSON)
		req.Header.Add("Content-Type", acceptJSON)
	}
	return req, nil
}
