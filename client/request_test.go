package client

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeRequest(t *testing.T) {
	t.Run("default headers should be set", func(t *testing.T) {
		req, err := makeRequest(context.Background(), "some.com/base/", execPath, http.MethodPost, &bytes.Buffer{})
		assert.NoError(t, err)
		assert.Equal(t, acceptJSON, req.Header.Get("Accept"))
		assert.Equal(t, acceptJSON, req.Header.Get("Content-Type"))
	})
	t.Run("stream headers should be set", func(t *testing.T) {
		streamPaths := []string{queryStreamPath, insertsStreamPath}
		for _, streamPath := range streamPaths {
			req, err := makeRequest(context.Background(), "some.com/base/", streamPath, http.MethodPost, &bytes.Buffer{})
			assert.NoError(t, err)
			assert.Equal(t, acceptDelim, req.Header.Get("Accept"))
			assert.Equal(t, acceptDelim, req.Header.Get("Content-Type"))
		}
	})
}
