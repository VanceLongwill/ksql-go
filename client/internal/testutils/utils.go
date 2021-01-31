package testutils

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/http2"
)

// Server starts an insecure http2 enabled server for testing a route
func Server(pattern string, mock http.HandlerFunc) *httptest.Server {
	handler := http.NewServeMux()
	handler.HandleFunc(pattern, mock)

	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = true
	srv.TLS = srv.Config.TLSConfig
	return srv
}

// ClientForServer returns a http client compatible with the http2 test server
func ClientForServer(srv *httptest.Server) *http.Client {
	tr := &http.Transport{TLSClientConfig: srv.Config.TLSConfig}
	if err := http2.ConfigureTransport(tr); err != nil {
		panic(fmt.Errorf("Failed to configure http2 transport: %v", err))
	}
	tr.TLSClientConfig.InsecureSkipVerify = true
	return &http.Client{Transport: tr}
}

// StreamingHandler matches an initial JSON request body with the given in param, then
// writes each of the given outs params to the response waiting 100ms inbetween each
func StreamingHandler(t *testing.T, in interface{}, outs ...interface{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		v := reflect.New(reflect.TypeOf(in).Elem()).Interface()
		err := json.NewDecoder(r.Body).Decode(v)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, in, v)
		for _, out := range outs {
			time.Sleep(time.Millisecond * 100)
			err = json.NewEncoder(w).Encode(out)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

// Handler returns a http handler which decodes and matches JSON request bodies with the in interface{},
// and writes the out interface{} as JSON to the response writer
func Handler(t *testing.T, in interface{}, out interface{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		v := reflect.New(reflect.TypeOf(in).Elem()).Interface()
		err := json.NewDecoder(r.Body).Decode(v)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, in, v)
		err = json.NewEncoder(w).Encode(out)
		if err != nil {
			t.Fatal(err)
		}
	}
}
