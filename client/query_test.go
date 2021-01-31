package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vancelongwill/ksql/client/internal/testutils"
)

func TestParseSchemaKeys(t *testing.T) {
	testCases := []struct {
		name   string
		raw    string
		parsed []string
	}{
		{
			"when there are no schema keys present",
			"asomsdjhaksdjhaskdh",
			nil,
		},
		{
			"when there is one schema key present",
			"`somekey`",
			[]string{"somekey"},
		},
		{
			"when there are multiply schema keys present",
			"`somekey`, `b`, `c`",
			[]string{"somekey", "b", "c"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseSchemaKeys(tc.raw)
			assert.Equal(t, tc.parsed, got)
		})
	}
}

func TestQueryError(t *testing.T) {
	t.Run("given a result with no message", func(t *testing.T) {
		err := &QueryError{map[string]interface{}{"key": "value"}}
		assert.EqualError(t, err, "an unknown error occurred")
	})
	t.Run("given a result with a message key", func(t *testing.T) {
		msg := "some specific error"
		err := &QueryError{map[string]interface{}{"message": msg}}
		assert.EqualError(t, err, msg)
	})
}

func TestQuery(t *testing.T) {
	t.Run("when the server returns a statement error", func(t *testing.T) {
		payload := QueryPayload{
			KSQL:              "SELECT * FROM pageviews;",
			StreamsProperties: NewStreamsProperties(OffsetEarliest),
		}
		msg := "something went wrong"
		results := map[string]interface{}{
			"message": msg,
		}
		srv := testutils.Server(
			queryPath, testutils.Handler(t, &payload, &results),
		)
		srv.StartTLS()
		defer srv.Close()
		c := New(srv.URL, WithHTTPClient(testutils.Client()))
		got, err := c.Query(context.Background(), payload)
		assert.EqualError(t, err, msg)
		assert.Nil(t, got)
	})
	t.Run("when the server returns a a set of results", func(t *testing.T) {
		payload := QueryPayload{
			KSQL:              "SELECT * FROM pageviews;",
			StreamsProperties: NewStreamsProperties(OffsetEarliest),
		}
		results := []map[string]interface{}{
			{
				"id": float64(123),
			},
			{
				"id": float64(1234),
			},
			{
				"id": float64(12345),
			},
		}
		srv := testutils.Server(
			queryPath, testutils.Handler(t, &payload, &results),
		)
		srv.StartTLS()
		defer srv.Close()
		c := New(srv.URL, WithHTTPClient(testutils.Client()))
		got, err := c.Query(context.Background(), payload)
		assert.NoError(t, err)
		assert.Equal(t, unset, got.columns.count)
		assert.Equal(t, got.res, results[1:])
	})
	t.Run("when the server returns a a set of results with a header row", func(t *testing.T) {
		payload := QueryPayload{
			KSQL:              "SELECT * FROM pageviews;",
			StreamsProperties: NewStreamsProperties(OffsetEarliest),
		}
		results := []map[string]interface{}{
			{
				"header": map[string]interface{}{
					"schema": "`id`",
				},
			},
			{
				"id": float64(1234),
			},
			{
				"id": float64(12345),
			},
		}
		srv := testutils.Server(
			queryPath, testutils.Handler(t, &payload, &results),
		)
		srv.StartTLS()
		defer srv.Close()
		c := New(srv.URL, WithHTTPClient(testutils.Client()))
		got, err := c.Query(context.Background(), payload)
		assert.NoError(t, err)
		assert.Equal(t, 1, got.columns.count)
		assert.Equal(t, []string{"id"}, got.columns.names)
		assert.Equal(t, got.res, results[1:])
	})
}
