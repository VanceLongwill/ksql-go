package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vancelongwill/ksql-go/client/internal/testutils"
)

func TestQueryStream(t *testing.T) {
	t.Run("it should process header rows", func(t *testing.T) {
		payload := QueryStreamPayload{
			KSQL: `SELECT * FROM pageviews_by_region
			       WHERE regionId = 'Region_1'
			       AND 1570051876000 <= WINDOWSTART
			       AND WINDOWEND <= 1570138276000;`,
			Properties: NewStreamsProperties(ExactlyOnce),
		}
		result := QueryResultHeader{
			ColumnNames: []string{"a", "b", "c"},
			ColumnTypes: []string{"STRING", "BIGINT", "BOOLEAN"},
		}
		srv := testutils.Server(
			queryStreamPath, testutils.Handler(t, &payload, &result),
		)
		srv.StartTLS()
		defer srv.Close()
		c := New(srv.URL, WithHTTPClient(testutils.Client()))
		got, err := c.QueryStream(context.Background(), payload)
		assert.NoError(t, err)
		assert.Equal(t, len(result.ColumnNames), got.columns.count)
		assert.Equal(t, result.ColumnNames, got.columns.names)
		assert.NotNil(t, got)
		ksqldb := c.(*ksqldb)
		assert.Len(t, ksqldb.rows, 1)
	})
	t.Run("it should stream rows", func(t *testing.T) {
		payload := QueryStreamPayload{
			KSQL: `SELECT * FROM pageviews_by_region
			       WHERE regionId = 'Region_1'
			       AND 1570051876000 <= WINDOWSTART
			       AND WINDOWEND <= 1570138276000;`,
			Properties: NewStreamsProperties(ExactlyOnce),
		}
		header := QueryResultHeader{
			ColumnNames: []string{"a", "b", "c"},
			ColumnTypes: []string{"STRING", "STRING", "BOOLEAN"},
		}
		results := []interface{}{
			header,
			[]interface{}{"first", "something", true},
			[]interface{}{"second", "something", true},
			[]interface{}{"third", "something", false},
		}
		srv := testutils.Server(
			queryStreamPath, testutils.StreamingHandler(t, &payload, results...),
		)
		srv.StartTLS()
		defer srv.Close()
		c := New(srv.URL, WithHTTPClient(testutils.Client()))
		got, err := c.QueryStream(context.Background(), payload)
		assert.NoError(t, err)
		assert.Equal(t, len(header.ColumnNames), got.columns.count)
		assert.Equal(t, header.ColumnNames, got.columns.names)
		assert.NotNil(t, got)
		ksqldb := c.(*ksqldb)
		assert.Len(t, ksqldb.rows, 1)

		for _, r := range results[1:] {
			dest := make([]interface{}, got.columns.count)
			err := got.Next(dest)
			assert.NoError(t, err)
			assert.Equal(t, r, dest)
		}
	})

	t.Run("when the context expires", func(t *testing.T) {
		payload := QueryStreamPayload{
			KSQL: `SELECT * FROM pageviews_by_region
			       WHERE regionId = 'Region_1'
			       AND 1570051876000 <= WINDOWSTART
			       AND WINDOWEND <= 1570138276000;`,
			Properties: NewStreamsProperties(ExactlyOnce),
		}
		header := QueryResultHeader{
			ColumnNames: []string{"a", "b", "c"},
			ColumnTypes: []string{"STRING", "STRING", "BOOLEAN"},
		}
		results := []interface{}{
			header,
			[]interface{}{"first", "something", true},
			[]interface{}{"second", "something", true},
			[]interface{}{"third", "something", false},
		}
		srv := testutils.Server(
			queryStreamPath, testutils.StreamingHandler(t, &payload, results...),
		)
		srv.StartTLS()
		defer srv.Close()
		c := New(srv.URL, WithHTTPClient(testutils.Client()))
		ctx, cancel := context.WithCancel(context.Background())
		got, err := c.QueryStream(ctx, payload)
		assert.NoError(t, err)
		assert.Equal(t, len(header.ColumnNames), got.columns.count)
		assert.Equal(t, header.ColumnNames, got.columns.names)
		assert.NotNil(t, got)
		ksqldb := c.(*ksqldb)
		assert.Len(t, ksqldb.rows, 1)

		dest := make([]interface{}, got.columns.count)
		err = got.Next(dest)
		assert.NoError(t, err)
		assert.Equal(t, results[1], dest)
		cancel()
		err = got.Next(dest)
		assert.Equal(t, ctx.Err(), err)
	})

	t.Run("when the rows have been closed", func(t *testing.T) {
		payload := QueryStreamPayload{
			KSQL: `SELECT * FROM pageviews_by_region
			       WHERE regionId = 'Region_1'
			       AND 1570051876000 <= WINDOWSTART
			       AND WINDOWEND <= 1570138276000;`,
			Properties: NewStreamsProperties(ExactlyOnce),
		}
		header := QueryResultHeader{
			ColumnNames: []string{"a", "b", "c"},
			ColumnTypes: []string{"STRING", "STRING", "BOOLEAN"},
		}
		results := []interface{}{
			header,
			[]interface{}{"first", "something", true},
			[]interface{}{"second", "something", true},
			[]interface{}{"third", "something", false},
		}
		srv := testutils.Server(
			queryStreamPath, testutils.StreamingHandler(t, &payload, results...),
		)
		srv.StartTLS()
		defer srv.Close()
		c := New(srv.URL, WithHTTPClient(testutils.Client()))
		got, err := c.QueryStream(context.Background(), payload)
		assert.NoError(t, err)
		assert.Equal(t, len(header.ColumnNames), got.columns.count)
		assert.Equal(t, header.ColumnNames, got.columns.names)
		assert.NotNil(t, got)
		ksqldb := c.(*ksqldb)
		assert.Len(t, ksqldb.rows, 1)
		dest := make([]interface{}, got.columns.count)
		err = got.Next(dest)
		assert.NoError(t, err)
		assert.Equal(t, results[1], dest)
		err = got.Close()
		assert.NoError(t, err)
		err = got.Next(dest)
		assert.Equal(t, ErrRowsClosed, err)
	})
}
