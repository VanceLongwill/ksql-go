package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListTablesExecAs(t *testing.T) {
	t.Run("given a non-nil ListTablesResult", func(t *testing.T) {
		res := &ExecResult{
			commonResult: commonResult{
				StatementText: "asdasd",
			},
			ListTablesResult: &ListTablesResult{
				Tables: []Table{
					{
						Name: "some_table",
					},
				},
			},
		}
		var list ListTablesResult
		assert.True(t, res.As(&list))
		assert.Equal(t, res.commonResult, list.commonResult, "the common result struct should be copied over")
		list.commonResult = commonResult{}
		assert.Equal(t, res.ListTablesResult, &list, "the list result should be copied over")
	})
	t.Run("given a nil ListTablesResult", func(t *testing.T) {
		res := &ExecResult{}
		var list ListTablesResult
		assert.False(t, res.As(&list))
	})
}

func TestListQueriesExecAs(t *testing.T) {
	t.Run("given a non-nil ListQueriesResult", func(t *testing.T) {
		res := &ExecResult{
			commonResult: commonResult{
				StatementText: "asdasd",
			},
			ListQueriesResult: &ListQueriesResult{
				Queries: []Query{
					{
						ID: "some_query",
					},
				},
			},
		}
		var list ListQueriesResult
		assert.True(t, res.As(&list))
		assert.Equal(t, res.commonResult, list.commonResult, "the common result struct should be copied over")
		list.commonResult = commonResult{}
		assert.Equal(t, res.ListQueriesResult, &list, "the list result should be copied over")
	})
	t.Run("given a nil ListQueriesResult", func(t *testing.T) {
		res := &ExecResult{}
		var list ListQueriesResult
		assert.False(t, res.As(&list))
	})
}

func TestListStreamsExecAs(t *testing.T) {
	t.Run("given a non-nil ListStreamsResult", func(t *testing.T) {
		res := &ExecResult{
			commonResult: commonResult{
				StatementText: "asdasd",
			},
			ListStreamsResult: &ListStreamsResult{
				Streams: []Stream{
					{
						Name: "some_stream",
					},
				},
			},
		}
		var list ListStreamsResult
		assert.True(t, res.As(&list))
		assert.Equal(t, res.commonResult, list.commonResult, "the common result struct should be copied over")
		list.commonResult = commonResult{}
		assert.Equal(t, res.ListStreamsResult, &list, "the list result should be copied over")
	})
	t.Run("given a nil ListStreamsResult", func(t *testing.T) {
		res := &ExecResult{}
		var list ListStreamsResult
		assert.False(t, res.As(&list))
	})
}

func TestListPropertiesExecAs(t *testing.T) {
	t.Run("given a non-nil ListPropertiesResult", func(t *testing.T) {
		res := &ExecResult{
			commonResult: commonResult{
				StatementText: "asdasd",
			},
			ListPropertiesResult: &ListPropertiesResult{
				Properties: map[string]string{
					"key": "value",
				},
			},
		}
		var list ListPropertiesResult
		assert.True(t, res.As(&list))
		assert.Equal(t, res.commonResult, list.commonResult, "the common result struct should be copied over")
		list.commonResult = commonResult{}
		assert.Equal(t, res.ListPropertiesResult, &list, "the list result should be copied over")
	})
	t.Run("given a nil ListPropertiesResult", func(t *testing.T) {
		res := &ExecResult{}
		var list ListPropertiesResult
		assert.False(t, res.As(&list))
	})
}
