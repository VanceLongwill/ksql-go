package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vancelongwill/ksql/client/internal/testutils"
)

func TestExec(t *testing.T) {
	testsCases := []struct {
		name    string
		payload ExecPayload
		results interface{}
		err     error
		got     []ExecResult
	}{
		{
			"it should send & receive multiple results",
			ExecPayload{
				KSQL: "DESCRIBE something;",
			},
			[]ExecResult{
				{
					DescribeResult: &DescribeResult{
						SourceDescription: SourceDescription{
							Name: "something",
						},
					},
				},
			},
			nil,
			[]ExecResult{
				{
					DescribeResult: &DescribeResult{
						SourceDescription: SourceDescription{
							Name: "something",
						},
					},
				},
			},
		},
		{
			"it should send and receive single results",
			ExecPayload{
				KSQL: "DESCRIBE something;",
			},
			ExecResult{
				DescribeResult: &DescribeResult{
					SourceDescription: SourceDescription{
						Name: "something",
					},
				},
			},
			nil,
			[]ExecResult{
				{
					DescribeResult: &DescribeResult{
						SourceDescription: SourceDescription{
							Name: "something",
						},
					},
				},
			},
		},
	}
	for _, tc := range testsCases {
		t.Run(tc.name, func(t *testing.T) {
			srv := testutils.Server(
				execPath, testutils.Handler(t, &tc.payload, &tc.results),
			)
			srv.StartTLS()
			defer srv.Close()
			c := New(srv.URL, WithHTTPClient(testutils.Client()))
			got, err := c.Exec(context.Background(), tc.payload)
			assert.Equal(t, tc.err, err)
			assert.Equal(t, tc.got, got)
		})
	}
}

func TestSingleExec(t *testing.T) {
	t.Run("when an empty result list is received", func(t *testing.T) {
		payload := ExecPayload{}
		results := make([]ExecResult, 0)
		srv := testutils.Server(
			execPath, testutils.Handler(t, &payload, &results),
		)
		srv.StartTLS()
		defer srv.Close()
		c := New(srv.URL, WithHTTPClient(testutils.Client())).(*ksqldb)
		_, err := c.singleExec(context.Background(), payload)
		assert.Error(t, err)
	})
	t.Run("when more than one result is received", func(t *testing.T) {
		payload := ExecPayload{}
		results := make([]ExecResult, 3)
		srv := testutils.Server(
			execPath, testutils.Handler(t, &payload, &results),
		)
		srv.StartTLS()
		defer srv.Close()
		c := New(srv.URL, WithHTTPClient(testutils.Client())).(*ksqldb)
		_, err := c.singleExec(context.Background(), payload)
		assert.Error(t, err)
	})
	t.Run("when only one result is received", func(t *testing.T) {
		payload := ExecPayload{}
		results := []ExecResult{
			{
				commonResult: commonResult{
					StatementText: "something",
				},
			},
		}
		srv := testutils.Server(
			execPath, testutils.Handler(t, &payload, &results),
		)
		srv.StartTLS()
		defer srv.Close()
		c := New(srv.URL, WithHTTPClient(testutils.Client())).(*ksqldb)
		got, err := c.singleExec(context.Background(), payload)
		assert.NoError(t, err)
		assert.Equal(t, results[0], got)
	})
}
